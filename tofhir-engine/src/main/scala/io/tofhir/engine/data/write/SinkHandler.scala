package io.tofhir.engine.data.write

import com.typesafe.scalalogging.Logger
import io.tofhir.engine.model.{FhirMappingErrorCodes, FhirMappingJobResult, FhirMappingResult}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.util.CollectionAccumulator

object SinkHandler {
  val logger: Logger = Logger(this.getClass)

  /**
   *
   * @param spark
   * @param jobId
   * @param mappingUrl
   * @param df
   * @param resourceWriter
   */
  def writeBatch(spark: SparkSession, jobId: String, mappingUrl: Option[String], df: Dataset[FhirMappingResult], resourceWriter: BaseFhirWriter): Unit = {
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
      val accumName = s"$jobId:${mappingUrl.map(u => s"$u:").getOrElse("")}fhirWritingProblems"
      val fhirWriteProblemsAccum: CollectionAccumulator[FhirMappingResult] = spark.sparkContext.collectionAccumulator[FhirMappingResult](accumName)
      fhirWriteProblemsAccum.reset()
      //Write the FHIR resources
      resourceWriter.write(spark, mappedResults, fhirWriteProblemsAccum)
      //Get the not written resources
      val notWrittenResources = fhirWriteProblemsAccum.value
      val numOfNotWritten = notWrittenResources.size()

      //Log the job result
      val jobResult = FhirMappingJobResult(jobId, mappingUrl, numOfInvalids, numOfNotMapped, numOfFhirResources, numOfNotWritten)
      logger.info(jobResult.toLogstashMarker, jobResult.toString)

      //Log the mapping errors
      if (numOfNotMapped > 0 || numOfInvalids > 0) {
        mappingErrors.union(invalidInputs).foreach(r =>
          logger.warn(r.toLogstashMarker, r.toString)
        )
      }
      if (numOfNotWritten > 0)
        notWrittenResources.forEach(r =>
          logger.warn(r.toLogstashMarker, r.toString)
        )
      //Unpersist the data frame
      df.unpersist()
    } catch {
      case t: Throwable =>
        val jobResult = FhirMappingJobResult(jobId, mappingUrl)
        logger.error(jobResult.toLogstashMarker, jobResult.toString, t)
        throw t
    }
  }

  /**
   *
   * @param spark
   * @param jobId
   * @param df
   * @param resourceWriter
   * @return
   */
  def writeStream(spark: SparkSession, jobId: String, df: Dataset[FhirMappingResult], resourceWriter: BaseFhirWriter): StreamingQuery = {
    val datasetWrite = (dataset: Dataset[FhirMappingResult], batchN: Long) =>
      writeBatch(spark, jobId, None, dataset, resourceWriter)

    df
      .writeStream
      .foreachBatch(datasetWrite)
      .start()
  }

}
