package io.tofhir.engine.execution.processing

import io.tofhir.engine.model.{FhirMappingErrorCodes, FhirMappingJobExecution, FhirMappingResult}
import io.tofhir.engine.util.FileUtils.FileExtensions
import org.apache.hadoop.fs.FileUtil
import org.apache.spark.sql.functions.{col, from_json, schema_of_json}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import java.io.File
import java.util

/**
 * This class persists [[FhirMappingResult]]s that produced some error during the mapping process.
 */
object ErroneousRecordWriter {

  /**
   * Archive data sources row by row based on error types (e.g. invalid input, mapping error, invalid resource) to configured folder
   *
   * @param spark                 spark session to create dataset from not written resources
   * @param mappingJobExecution   job execution to get output directory of data sources with errors
   * @param mappingTaskName       to create directory for each mappingTask name within the job execution
   * @param notWrittenResources   created fhir resources that cannot be written to fhir server
   * @param mappingErrors         data sources that got error because of the mapping definition
   * @param invalidInputs         data sources that got error because of invalid input
   */
  def saveErroneousRecords(spark: SparkSession,
                           mappingJobExecution: FhirMappingJobExecution,
                           mappingTaskName: Option[String],
                           notWrittenResources: util.List[FhirMappingResult],
                           mappingErrors: Dataset[FhirMappingResult],
                           invalidInputs: Dataset[FhirMappingResult]): Unit = {
    if (mappingJobExecution.saveErroneousRecords) {
      if (!invalidInputs.isEmpty) {
        this.writeErroneousDataset(mappingJobExecution, invalidInputs, mappingTaskName.get, FhirMappingErrorCodes.INVALID_INPUT)
      }
      if (!mappingErrors.isEmpty) {
        this.writeErroneousDataset(mappingJobExecution, mappingErrors, mappingTaskName.get, FhirMappingErrorCodes.MAPPING_ERROR)
      }
      if (!notWrittenResources.isEmpty) {
        import spark.implicits._
        val notWrittenResourcesDs = spark.createDataset[FhirMappingResult](notWrittenResources)
        this.writeErroneousDataset(mappingJobExecution, notWrittenResourcesDs, mappingTaskName.get, FhirMappingErrorCodes.INVALID_RESOURCE)
      }
    }
  }

  /**
   * Writes the dataset to the errorOutputDirectory. Directory structure:
   * error-folder-path\<error-type>\job-<jobId>\execution-<executionId>\<mappingTaskName>\<random-generated-name-by-spark>.csv
   *
   * @param mappingJobExecution   job execution to get output directory of data sources with errors
   * @param dataset               filtered dataset of data sources with errors to write to configured folder
   * @param mappingTaskName       to create directory for each mapping name within the job execution
   * @param errorType             one of invalid_input, mapping_error, invalid_resource
   */
  private def writeErroneousDataset(mappingJobExecution: FhirMappingJobExecution,
                                    dataset: Dataset[FhirMappingResult],
                                    mappingTaskName: String,
                                    errorType: String): Unit = {
    val outputPath = mappingJobExecution.getErrorOutputDirectory(mappingTaskName, errorType)
    val schema = schema_of_json(dataset.rdd.takeSample(withReplacement = false, num = 1).head.source.get)

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
