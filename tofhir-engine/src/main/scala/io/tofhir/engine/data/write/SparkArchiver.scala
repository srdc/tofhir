package io.tofhir.engine.data.write

import io.tofhir.engine.model.{FhirMappingErrorCodes, FhirMappingJobExecution, FhirMappingResult}
import io.tofhir.engine.util.FileUtils.FileExtensions
import org.apache.hadoop.fs.FileUtil
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, from_json, schema_of_json}

import java.io.File
import java.util

/**
 * Spark archiver for writing data sources of failed mappings to configured folder
 */
object SparkArchiver {

  /**
   * Archive data sources row by row based on error types (e.g. invalid input, mapping error, invalid resource) to configured folder
   * @param spark spark session to create dataset from not written resources
   * @param mappingJobExecution job execution to get output directory of data sources with errors
   * @param mappingUrl to create directory for each mapping url within the job execution
   * @param notWrittenResources created fhir resources that cannot be written to fhir server
   * @param mappingErrors data sources that got error because of the mapping definition
   * @param invalidInputs data sources that got error because of invalid input
   */
  def archiveDataSourcesOfFailedMappings(spark: SparkSession,
                                         mappingJobExecution: FhirMappingJobExecution,
                                         mappingUrl: Option[String],
                                         notWrittenResources: util.List[FhirMappingResult],
                                         mappingErrors: Dataset[FhirMappingResult],
                                         invalidInputs: Dataset[FhirMappingResult]): Unit = {
    if (!invalidInputs.isEmpty) {
      this.writeDatasetToConfiguredFolder(mappingJobExecution, invalidInputs, mappingUrl.get, FhirMappingErrorCodes.INVALID_INPUT)
    }
    if (!mappingErrors.isEmpty) {
      this.writeDatasetToConfiguredFolder(mappingJobExecution, mappingErrors, mappingUrl.get, FhirMappingErrorCodes.MAPPING_ERROR)
    }
    if (!notWrittenResources.isEmpty) {
      import spark.implicits._
      val notWrittenResourcesDs = spark.createDataset[FhirMappingResult](notWrittenResources)
      this.writeDatasetToConfiguredFolder(mappingJobExecution, notWrittenResourcesDs, mappingUrl.get, FhirMappingErrorCodes.INVALID_RESOURCE)
    }
  }

  /**
   * Writes the dataset to the configured folder
   * @param mappingJobExecution job execution to get output directory of data sources with errors
   * @param dataset filtered dataset of data sources with errors to write to configured folder
   * @param mappingUrl to create directory for each mapping url within the job execution
   * @param errorType one of invalid_input, mapping_error, invalid_resource
   */
  private def writeDatasetToConfiguredFolder(mappingJobExecution: FhirMappingJobExecution,
                                             dataset: Dataset[FhirMappingResult],
                                             mappingUrl: String,
                                             errorType: String): Unit = {
    val outputPath = mappingJobExecution.getErrorOutputDirectory(mappingUrl, errorType)
    val schema = schema_of_json(dataset.collect().head.source.get)

    dataset
      .withColumn("jsonData", from_json(col("source"), schema))
      .select("jsonData.*")
      .coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv(outputPath)

    // Remove all files except the CSV file (to remove .crc files)
    val srcFiles = FileUtil.listFiles(new File(outputPath))
      .filterNot(f => f.getPath.endsWith(FileExtensions.CSV.toString))
    srcFiles.foreach(f => f.delete())
  }

}
