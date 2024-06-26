package io.tofhir.engine.data.write

import com.typesafe.scalalogging.Logger
import io.tofhir.engine.execution.ErroneousRecordWriter
import io.tofhir.engine.model._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.util.CollectionAccumulator

import java.util

object SinkHandler {
  val logger: Logger = Logger(this.getClass)

  /**
   *
   * @param spark
   * @param mappingJobExecution
   * @param mappingUrl
   * @param df
   * @param resourceWriter
   */
  def writeBatch(spark: SparkSession, mappingJobExecution: FhirMappingJobExecution, mappingUrl: Option[String], df: Dataset[FhirMappingResult], resourceWriter: BaseFhirWriter): Unit = {
    //Cache the dataframe
    df.cache()
    //Filter out the errors
    val invalidInputs = df.filter(_.error.map(_.code).contains(FhirMappingErrorCodes.INVALID_INPUT))
    val mappingErrors = df.filter(_.error.exists(_.code != FhirMappingErrorCodes.INVALID_INPUT))
    val mappedResults = df.filter(_.mappedResource.isDefined)
    //Create an accumulator to accumulate the results that cannot be written
    val accumName = s"${mappingJobExecution.jobId}:${mappingUrl.map(u => s"$u:").getOrElse("")}fhirWritingProblems"
    val fhirWriteProblemsAccum: CollectionAccumulator[FhirMappingResult] = spark.sparkContext.collectionAccumulator[FhirMappingResult](accumName)
    fhirWriteProblemsAccum.reset()
    //Write the FHIR resources
    resourceWriter.write(spark, mappedResults, fhirWriteProblemsAccum)
    logMappingJobResult(mappingJobExecution,mappingUrl,mappedResults,fhirWriteProblemsAccum.value,mappingErrors,invalidInputs)
    ErroneousRecordWriter.saveErroneousRecords(spark, mappingJobExecution, mappingUrl, fhirWriteProblemsAccum.value, mappingErrors, invalidInputs)
    //Unpersist the data frame
    df.unpersist()
  }

  /**
   *
   * @param spark
   * @param mappingJobExecution
   * @param df
   * @param resourceWriter
   * @param mappingUrl
   * @return
   */
  def writeStream(spark: SparkSession, mappingJobExecution: FhirMappingJobExecution, df: Dataset[FhirMappingResult], resourceWriter: BaseFhirWriter, mappingUrl: String): StreamingQuery = {
    val datasetWrite = (dataset: Dataset[FhirMappingResult], batchN: Long) => try {
      writeBatch(spark, mappingJobExecution, Some(mappingUrl), dataset, resourceWriter)
    } catch {
      case e: Throwable =>
        logger.error(s"Streaming batch resulted in error for project: ${mappingJobExecution.projectId}, job: ${mappingJobExecution.jobId}, execution: ${mappingJobExecution.id}, mapping: $mappingUrl", e.getMessage)
    }

    df
      .writeStream
      // We need to provide explicit checkpoints. If not, Spark will use the same checkpoint directory, which mixes up the offsets for different streams.
      // We create a new checkpoint directory per job and per mapping task included in the jobs.
      .option("checkpointLocation", mappingJobExecution.getCheckpointDirectory(mappingUrl))
      .foreachBatch(datasetWrite)
      .start()
  }

  /**
   * Logs mapping job results including the problems regarding to source data, mapping and generated FHIR resources.
   *
   * @param mappingJobExecution The mapping job execution
   * @param mappingUrl The url of executed mapping
   * @param fhirResources written FHIR resources to the configured server
   * @param notWrittenResources The FHIR resource errors
   * @param mappingErrors The mapping errors
   * @param invalidInputs The source data errors
   * */
  private def logMappingJobResult(mappingJobExecution:FhirMappingJobExecution,
                                  mappingUrl:Option[String],
                                  fhirResources: Dataset[FhirMappingResult],
                                  notWrittenResources:util.List[FhirMappingResult],
                                  mappingErrors:Dataset[FhirMappingResult],
                                  invalidInputs:Dataset[FhirMappingResult]) = {
    //Get the not written resources
    val numOfInvalids = invalidInputs.count()
    val numOfNotMapped = mappingErrors.count()
    val numOfNotWritten = notWrittenResources.size()
    val numOfFhirResources = fhirResources.count()
    val numOfWritten = numOfFhirResources - numOfNotWritten

    //Log the job result
    val jobResult = FhirMappingJobResult(mappingJobExecution, mappingUrl, numOfInvalids, numOfNotMapped, numOfWritten, numOfNotWritten)
    logger.info(jobResult.toMapMarker, jobResult.toString)

    // Log the mapping and invalid input errors
    if (numOfNotMapped > 0 || numOfInvalids > 0) {
      mappingErrors.union(invalidInputs).foreach(r =>
        logger.warn(r.copy(executionId = Some(mappingJobExecution.id)).toMapMarker,
          r.copy(executionId = Some(mappingJobExecution.id)).toString)
      )
    }
    if (numOfNotWritten > 0)
      notWrittenResources.forEach(r =>
        logger.warn(r.copy(executionId = Some(mappingJobExecution.id)).toMapMarker,
          r.copy(executionId = Some(mappingJobExecution.id)).toString)
      )
  }
}
