package io.tofhir.engine.data.write

import java.util
import com.typesafe.scalalogging.Logger
import io.tofhir.engine.model.{FhirMappingError, FhirMappingErrorCodes, FhirMappingException, FhirMappingInvalidResourceException, FhirMappingJobExecution, FhirMappingJobResult, FhirMappingResult}
import org.apache.spark.SparkException
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.util.CollectionAccumulator

import java.sql.Timestamp
import java.time.Instant

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
    try {
      //Cache the dataframe
      df.cache()
      //Filter out the errors
      val invalidInputs = df.filter(_.error.map(_.code).contains(FhirMappingErrorCodes.INVALID_INPUT))
      val mappingErrors = df.filter(_.error.exists(_.code != FhirMappingErrorCodes.INVALID_INPUT))
      val mappedResults = df.filter(_.mappedResource.isDefined)
      val numOfInvalids = invalidInputs.count()
      val numOfNotMapped = mappingErrors.count()
      val numOfFhirResources = mappedResults.count()
      //Create an accumulator to accumulate the results that cannot be written
      val accumName = s"${mappingJobExecution.jobId}:${mappingUrl.map(u => s"$u:").getOrElse("")}fhirWritingProblems"
      val fhirWriteProblemsAccum: CollectionAccumulator[FhirMappingResult] = spark.sparkContext.collectionAccumulator[FhirMappingResult](accumName)
      fhirWriteProblemsAccum.reset()
      //Write the FHIR resources
      try {
        resourceWriter.write(spark, mappedResults, fhirWriteProblemsAccum)
      } catch {
        case t:Throwable => {
          // handle the exception caused by invalid mapping results
          t.getCause match {
            case e:FhirMappingInvalidResourceException  =>
              logMappingJobResult(mappingJobExecution,mappingUrl,numOfFhirResources,e.getProblems,mappingErrors,invalidInputs)
          }
          throw t
        }
      }
      logMappingJobResult(mappingJobExecution,mappingUrl,numOfFhirResources,fhirWriteProblemsAccum.value,mappingErrors,invalidInputs)
      //Unpersist the data frame
      df.unpersist()
    } catch {
      case t:Throwable => {
        t.getCause match {
          // FhirMappingInvalidResourceException is already handled above
          case e:FhirMappingInvalidResourceException => None
          // FhirMappingException is already handled and logged by Spark while running the mapping
          case e:FhirMappingException => None
          // Handle mismatching of schema column names and CSV column names
          case e: IllegalArgumentException =>
            val csvHeaderError = FhirMappingResult(
              jobId = mappingJobExecution.jobId,
              mappingUrl = mappingUrl.getOrElse(""),
              timestamp = Timestamp.from(Instant.now()),
              error = Some(FhirMappingError(
                code = FhirMappingErrorCodes.INVALID_INPUT,
                description = "Schema column names and CSV column names does not match!!"
              )),
              executionId = Some(mappingJobExecution.id)
            )
            logger.error(csvHeaderError.toLogstashMarker, csvHeaderError.toString, t)
            val jobResult = FhirMappingJobResult(mappingJobExecution, mappingUrl)
            logger.error(jobResult.toLogstashMarker, jobResult.toString, t)
          // log the mapping job result and exception for the rest
          case _ =>
            val jobResult = FhirMappingJobResult(mappingJobExecution, mappingUrl)
            logger.error(jobResult.toLogstashMarker, jobResult.toString, t)
        }
      }
    }
  }

  /**
   *
   * @param spark
   * @param mappingJobExecution
   * @param df
   * @param resourceWriter
   * @return
   */
  def writeStream(spark: SparkSession, mappingJobExecution: FhirMappingJobExecution, df: Dataset[FhirMappingResult], resourceWriter: BaseFhirWriter): StreamingQuery = {
    val datasetWrite = (dataset: Dataset[FhirMappingResult], batchN: Long) =>
      writeBatch(spark, mappingJobExecution, None, dataset, resourceWriter)

    df
      .writeStream
      .foreachBatch(datasetWrite)
      .start()
  }

  /**
   * Logs mapping job results including the problems regarding to source data, mapping and generated FHIR resources.
   *
   * @param mappingJobExecution The mapping job execution
   * @param mappingUrl The url of executed mapping
   * @param numOfFhirResources The number of written FHIR resources to the configured server
   * @param notWrittenResources The FHIR resource errors
   * @param mappingErrors The mapping errors
   * @param invalidInputs The source data errors
   * */
  private def logMappingJobResult(mappingJobExecution:FhirMappingJobExecution,
                                  mappingUrl:Option[String],
                                  numOfFhirResources:Long,
                                  notWrittenResources:util.List[FhirMappingResult],
                                  mappingErrors:Dataset[FhirMappingResult],
                                  invalidInputs:Dataset[FhirMappingResult]) = {
    //Get the not written resources
    val numOfInvalids = invalidInputs.count()
    val numOfNotMapped = mappingErrors.count()
    val numOfNotWritten = notWrittenResources.size()
    val numOfWritten = numOfFhirResources - numOfNotWritten

    //Log the job result
    val jobResult = FhirMappingJobResult(mappingJobExecution, mappingUrl, numOfInvalids, numOfNotMapped, numOfWritten, numOfNotWritten)
    logger.info(jobResult.toLogstashMarker, jobResult.toString)

    // Log the mapping and invalid input errors
    if (numOfNotMapped > 0 || numOfInvalids > 0) {
      mappingErrors.union(invalidInputs).foreach(r =>
        logger.warn(r.copy(executionId = Some(mappingJobExecution.id)).toLogstashMarker,
          r.copy(executionId = Some(mappingJobExecution.id)).toString)
      )
    }
    if (numOfNotWritten > 0)
      notWrittenResources.forEach(r =>
        logger.warn(r.copy(executionId = Some(mappingJobExecution.id)).toLogstashMarker,
          r.copy(executionId = Some(mappingJobExecution.id)).toString)
      )
  }
}
