package io.ignifyr.engine.data.write

import com.typesafe.scalalogging.Logger
import io.ignifyr.engine.execution.log.ExecutionLogger
import io.ignifyr.engine.execution.processing.ErroneousRecordWriter
import io.ignifyr.engine.model._
import io.ignifyr.engine.spi.{
  ExtensionHints,
  ExtensionRegistry,
  MappedResourceValidator,
  MissingCapabilityException,
  ResourceValidatorProvider,
  ValidationIssue
}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.util.CollectionAccumulator
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import java.util
import scala.util.control.NonFatal

object SinkHandler {
  val logger: Logger = Logger(this.getClass)

  /**
   * Writes the FHIR mapping results to the specified resource writer.
   *
   * @param spark The SparkSession instance.
   * @param mappingJobExecution The execution context of the FHIR mapping job.
   * @param mappingTaskName An optional name for the mapping.
   * @param df The DataFrame containing FHIR mapping results.
   * @param resourceWriter The writer instance to write the FHIR resources.
   */
  def writeMappingResult(
      spark: SparkSession,
      mappingJobExecution: FhirMappingJobExecution,
      mappingTaskName: String,
      df: Dataset[FhirMappingResult],
      resourceWriter: BaseSinkWriter
  ): Unit =
    writeMappingResult(spark, mappingJobExecution, mappingTaskName, df, resourceWriter, ExtensionRegistry.resourceValidator)

  /**
   * [[writeMappingResult]] with the resource validator provider given explicitly (the installed one by default), so
   * that the validation step can be tested with a stub provider.
   */
  private[ignifyr] def writeMappingResult(
      spark: SparkSession,
      mappingJobExecution: FhirMappingJobExecution,
      mappingTaskName: String,
      df: Dataset[FhirMappingResult],
      resourceWriter: BaseSinkWriter,
      resourceValidator: => Option[ResourceValidatorProvider]
  ): Unit = {
    // Cache the dataframe
    df.cache()
    // Filter out the errors
    val invalidInputs = df.filter(_.error.map(_.code).contains(FhirMappingErrorCodes.INVALID_INPUT))
    val mappingErrors = df.filter(_.error.exists(_.code != FhirMappingErrorCodes.INVALID_INPUT))
    val mappedResults = df.filter(_.mappedFhirResource.flatMap(_.mappedResource).isDefined)
    // Create an accumulator to accumulate the results that cannot be written
    val accumName = s"${mappingJobExecution.jobId}:$mappingTaskName:fhirWritingProblems"
    val fhirWriteProblemsAccum: CollectionAccumulator[FhirMappingResult] =
      spark.sparkContext.collectionAccumulator[FhirMappingResult](accumName)
    fhirWriteProblemsAccum.reset()
    // Validate the mapped resources if the job asks for it. The invalid ones are reported like the resources the sink
    // rejects (they count as not written) and only the valid ones are handed to the writer.
    val validatedResults: Option[Dataset[(FhirMappingResult, Option[String])]] =
      if (mappingJobExecution.validateResources) {
        val validator = resourceValidator
          .getOrElse(throw MissingCapabilityException(ExtensionHints.describeResourceValidator))
          .validatorFor(mappingJobExecution, mappingTaskName)
        Some(validateMappedResults(spark, mappedResults, validator, fhirWriteProblemsAccum))
      } else None
    val resourcesToWrite = validatedResults match {
      case Some(validated) =>
        import spark.implicits._
        validated.filter(_._2.isEmpty).map(_._1)
      case None => mappedResults
    }
    // Write the FHIR resources, unless the execution skips writing (the accumulator then stays empty, so the mapped
    // resources are logged as numOfFhirResources with no failed writes)
    if (mappingJobExecution.isWriteSkipped)
      logger.info(
        s"Skipping the write of the mapped resources for mappingTask '$mappingTaskName' of execution '${mappingJobExecution.id}'."
      )
    else
      resourceWriter.write(spark, resourcesToWrite, fhirWriteProblemsAccum)
    logMappingJobResult(
      mappingJobExecution,
      mappingTaskName,
      mappedResults,
      fhirWriteProblemsAccum.value,
      mappingErrors,
      invalidInputs
    )
    // All actions on the mapped chunk are done; let the lookup observers publish what they recorded for it
    ExtensionRegistry.notifyLookupChunkCompleted(mappingJobExecution, mappingTaskName)
    ErroneousRecordWriter.saveErroneousRecords(
      spark,
      mappingJobExecution,
      mappingTaskName,
      fhirWriteProblemsAccum.value,
      mappingErrors,
      invalidInputs
    )
    // Unpersist the data frames
    validatedResults.foreach(_.unpersist())
    df.unpersist()
  }

  /**
   * Fails fast, before anything is mapped, when the execution asks for the validation of the mapped resources but no
   * resource validator is installed.
   *
   * @throws MissingCapabilityException if the validation is asked for and no resource validator is installed
   */
  def requireResourceValidator(mappingJobExecution: FhirMappingJobExecution): Unit =
    if (mappingJobExecution.validateResources && ExtensionRegistry.resourceValidator.isEmpty)
      throw MissingCapabilityException(ExtensionHints.describeResourceValidator)

  /**
   * Validates the mapped resources and adds the invalid ones (with an [[FhirMappingErrorCodes.INVALID_RESOURCE]] error
   * listing the issues) to the problems accumulator.
   *
   * @return Every mapped result with the JSON of its issues if it is invalid (cached; the caller unpersists it)
   */
  private def validateMappedResults(
      spark: SparkSession,
      mappedResults: Dataset[FhirMappingResult],
      validator: MappedResourceValidator,
      problemsAccumulator: CollectionAccumulator[FhirMappingResult]
  ): Dataset[(FhirMappingResult, Option[String])] = {
    import spark.implicits._
    // Validate once: the dataset is read again for the invalid and for the valid results
    val validated = mappedResults.map(result => result -> invalidResourceIssues(validator, result)).cache()
    validated
      .filter(_._2.isDefined)
      .collect()
      .foreach { case (result, issues) =>
        problemsAccumulator.add(
          result.copy(error =
            Some(
              FhirMappingError(
                code = FhirMappingErrorCodes.INVALID_RESOURCE,
                description = "Resource does not conform to its profile(s)!",
                expression = issues
              )
            )
          )
        )
      }
    validated
  }

  /**
   * Validates the mapped resource of a result.
   *
   * @return The JSON of the validation issues if the resource is invalid (has an error), None if it conforms
   */
  private def invalidResourceIssues(validator: MappedResourceValidator, result: FhirMappingResult): Option[String] = {
    val issues =
      try result.mappedFhirResource.flatMap(_.mappedResource).map(validator.validate).getOrElse(Nil)
      catch {
        case NonFatal(t) =>
          Seq(ValidationIssue("fatal", "exception", Some(s"Resource could not be validated: ${t.getMessage}")))
      }
    if (issues.exists(_.isError)) Some(Serialization.write(issues)(DefaultFormats)) else None
  }

  /**
   * Logs mapping job results including the problems regarding to source data, mapping and generated FHIR resources.
   *
   * @param mappingJobExecution The mapping job execution
   * @param mappingTaskName The name of executed mappingTask
   * @param fhirResources written FHIR resources to the configured server
   * @param notWrittenResources The FHIR resource errors
   * @param mappingErrors The mapping errors
   * @param invalidInputs The source data errors
   * */
  private def logMappingJobResult(
      mappingJobExecution: FhirMappingJobExecution,
      mappingTaskName: String,
      fhirResources: Dataset[FhirMappingResult],
      notWrittenResources: util.List[FhirMappingResult],
      mappingErrors: Dataset[FhirMappingResult],
      invalidInputs: Dataset[FhirMappingResult]
  ) = {
    // Get the not written resources
    val numOfInvalids = invalidInputs.count()
    val numOfNotMapped = mappingErrors.count()
    val numOfNotWritten = notWrittenResources.size()
    val numOfFhirResources = fhirResources.count()
    val numOfWritten = numOfFhirResources - numOfNotWritten

    // Log the job result
    if (mappingJobExecution.isStreamingJob) {
      // Log the result for streaming mapping task execution
      ExecutionLogger.logExecutionResultForStreamingMappingTask(
        mappingJobExecution,
        mappingTaskName,
        numOfInvalids,
        numOfNotMapped,
        numOfWritten,
        numOfNotWritten
      )
    } else {
      // Log the result for batch execution
      ExecutionLogger.logExecutionResultForChunk(
        mappingJobExecution,
        mappingTaskName,
        numOfInvalids,
        numOfNotMapped,
        numOfWritten,
        numOfNotWritten
      )
    }

    // Log the mapping and invalid input errors
    if (numOfNotMapped > 0 || numOfInvalids > 0) {
      mappingErrors
        .union(invalidInputs)
        .foreach(r =>
          logger.warn(
            r.copy(executionId = Some(mappingJobExecution.id)).toMapMarker,
            r.copy(executionId = Some(mappingJobExecution.id)).toString
          )
        )
    }
    if (numOfNotWritten > 0)
      notWrittenResources.forEach(r =>
        logger.warn(
          r.copy(executionId = Some(mappingJobExecution.id)).toMapMarker,
          r.copy(executionId = Some(mappingJobExecution.id)).toString
        )
      )
  }
}
