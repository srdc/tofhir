package io.tofhir.engine.execution.processing

import io.tofhir.engine.model.{FhirMappingErrorCodes, FhirMappingJobExecution, FhirMappingResult}
import io.tofhir.engine.util.FileUtils.FileExtensions
import org.apache.spark.sql.functions.{col, from_json, schema_of_json}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import java.io.File
import java.util
import io.onfhir.util.JsonFormatter._

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
                           mappingTaskName: String,
                           notWrittenResources: util.List[FhirMappingResult],
                           mappingErrors: Dataset[FhirMappingResult],
                           invalidInputs: Dataset[FhirMappingResult]): Unit = {
    if (mappingJobExecution.saveErroneousRecords) {
      if (!invalidInputs.isEmpty) {
        this.writeErroneousDataset(mappingJobExecution, invalidInputs, mappingTaskName, FhirMappingErrorCodes.INVALID_INPUT)
      }
      if (!mappingErrors.isEmpty) {
        this.writeErroneousDataset(mappingJobExecution, mappingErrors, mappingTaskName, FhirMappingErrorCodes.MAPPING_ERROR)
      }
      if (!notWrittenResources.isEmpty) {
        import spark.implicits._
        val notWrittenResourcesDs = spark.createDataset[FhirMappingResult](notWrittenResources)
        this.writeErroneousDataset(mappingJobExecution, notWrittenResourcesDs, mappingTaskName, FhirMappingErrorCodes.INVALID_RESOURCE)
      }
    }
  }

  /**
   * Writes the dataset to the error output directory based on the error type and job details.
   * Each source (e.g., "mainSource") within the "source" field is extracted dynamically,
   * and its data is written into a separate subdirectory under the output path. The directory structure is as follows:
   *
   * error-folder-path\<error-type>\job-<jobId>\execution-<executionId>\<mappingTaskName>\<sourceName>\<random-generated-name-by-spark>.csv
   *
   * @param mappingJobExecution The job execution object, used to get the output directory of data sources with errors.
   * @param dataset             The filtered dataset of data sources with errors to be written to the configured folder.
   * @param mappingTaskName     The name of the mapping task, used to create a directory for each mapping within the job execution.
   * @param errorType           The type of error (e.g., invalid_input, mapping_error, invalid_resource).
   */
  private def writeErroneousDataset(mappingJobExecution: FhirMappingJobExecution,
                                    dataset: Dataset[FhirMappingResult],
                                    mappingTaskName: String,
                                    errorType: String): Unit = {
    val outputPath = mappingJobExecution.getErrorOutputDirectory(mappingTaskName, errorType)
    val schema = schema_of_json(dataset.rdd.takeSample(withReplacement = false, num = 1).head.source)

    // extract each source name (e.g., "mainSource", "secondarySource") from the dataset's "source" field
    val jsonColumns: Array[String] = dataset
      .select("source")
      .rdd
      .flatMap(row => row.getAs[String]("source").parseJson.values.keys) // parse JSON and get all keys from the "source" map
      .distinct() // remove duplicate source names
      .collect()

    // for each source, create and write a separate CSV file
    jsonColumns.foreach { sourceKey =>
      // extract the source data for the given sourceKey and flatten the selected source field
      val sourceDataset = dataset
        .withColumn("jsonData", from_json(col("source"), schema))
        .selectExpr(s"jsonData.$sourceKey.*")

      // write the extracted source data as a separate CSV file into the directory for that sourceKey
      sourceDataset
        .coalesce(1)
        .write
        .mode(SaveMode.Append)
        .option("header", "true")
        .csv(s"$outputPath/$sourceKey")
    }

    // remove all files except the CSV file (to remove .crc files)
    deleteNonCsvFiles(new File(outputPath))
  }

  /**
   * Recursively deletes non-CSV files (like .crc files) from the directory.
   *
   * @param dir The directory to clean up by removing non-CSV files.
   */
  private def deleteNonCsvFiles(dir: File): Unit = {
    if (dir.exists() && dir.isDirectory) {
      // get a list of files to delete, excluding CSV files
      val filesToDelete =  dir.listFiles()
        .filterNot(f => f.getPath.endsWith(FileExtensions.CSV.toString))
      // process each file in the current directory
      filesToDelete.foreach(file => {
        if (file.isFile) {
          // delete the file if it's a regular file
          file.delete()
        } else if (file.isDirectory) {
          // if it's a subdirectory, recursively clean it up
          deleteNonCsvFiles(file)
        }
      })
    }
  }
}
