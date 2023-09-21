package io.tofhir.engine.data.write

import io.tofhir.engine.config.ToFhirConfig
import io.tofhir.engine.model._
import io.tofhir.engine.util.FileUtils
import io.tofhir.engine.util.FileUtils.FileExtensions
import org.apache.hadoop.fs.FileUtil
import org.apache.spark.sql.functions.{col, from_json, schema_of_json}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import java.io.File
import java.net.URI
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.util

/**
 * Spark archiver for writing data sources of failed mappings to configured folder
 */
object SparkArchiver {

  /**
   * Archive data sources row by row based on error types (e.g. invalid input, mapping error, invalid resource) to configured folder
   *
   * @param spark               spark session to create dataset from not written resources
   * @param mappingJobExecution job execution to get output directory of data sources with errors
   * @param mappingUrl          to create directory for each mapping url within the job execution
   * @param notWrittenResources created fhir resources that cannot be written to fhir server
   * @param mappingErrors       data sources that got error because of the mapping definition
   * @param invalidInputs       data sources that got error because of invalid input
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
   *
   * @param mappingJobExecution job execution to get output directory of data sources with errors
   * @param dataset             filtered dataset of data sources with errors to write to configured folder
   * @param mappingUrl          to create directory for each mapping url within the job execution
   * @param errorType           one of invalid_input, mapping_error, invalid_resource
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

  /**
   * Move file type data sources to archive folder
   * @param df
   * @param mappingJobExecution
   */
  def moveFileToArchive(df: Dataset[FhirMappingResult], mappingJobExecution: FhirMappingJobExecution): Unit = {
    // get data folder path from first data source settings if its FileSystemSourceSettings
    mappingJobExecution.job.sourceSettings.head._2 match {
      case fileSystemSourceSettings: FileSystemSourceSettings =>
        val dataFolderPath = fileSystemSourceSettings.dataFolderPath
        if (fileSystemSourceSettings.asStream) {
          // get file names from df by grouping by path
          val schema = schema_of_json(df.collect().head.source.get)
          // uri paths are included in the dataframes for the streaming jobs
          val absoluteFilePaths = df
            .withColumn("jsonData", from_json(col("source"), schema))
            .select("jsonData.*")
            .groupBy(col("filename")).count().collect().map(x => x.getString(0))
          // create path object for each file paths e.g. file:///C:/dev/be/data-integration-suite/test-data/streaming-folder/patients/patients-invalid-input.csv
          val absolutePaths = absoluteFilePaths.map(x => Paths.get(new URI(x)))
          // create archive path
          absolutePaths.foreach(absolutePath => {
            // create a relative path to the context path of the engine for that file
            val relativePath = FileUtils.getPath("").toAbsolutePath.relativize(absolutePath)
            val archivePath = FileUtils.getPath(ToFhirConfig.engineConfig.archiveFolder, relativePath.toString)
            val archiveFile = new File(archivePath.toString)
            // create parent directories if not exists
            archiveFile.getParentFile.mkdirs()
            // move file to archive folder
            Files.move(absolutePath, archiveFile.toPath, StandardCopyOption.ATOMIC_MOVE)
          })
        } else {
          // find specific file path by matching mapping urls in the job and df
          df.select("mappingUrl")
            .groupBy(col("mappingUrl")).count().collect().map(x => x.getString(0))
            .foreach((mappingUrl: String) => {
              // find file name by mapping url
              val paths = mappingJobExecution.job.mappings.find(x => x.mappingRef == mappingUrl).get.sourceContext.map(fhirMappingSourceContextMap => {
                fhirMappingSourceContextMap._2 match {
                  case fileSystemSource: FileSystemSource => fileSystemSource.path
                }
              })
              paths.foreach(relativePath => {
                val filePath = Paths.get(dataFolderPath, relativePath)
                val archivePath = FileUtils.getPath(ToFhirConfig.engineConfig.archiveFolder, filePath.toString)
                val archiveFile = new File(archivePath.toString)
                // create parent directories if not exists
                archiveFile.getParentFile.mkdirs()
                // move file to archive folder
                Files.move(filePath, archiveFile.toPath, StandardCopyOption.ATOMIC_MOVE)
              })
            })
          FileUtils.getPath(dataFolderPath)
        }
    }
  }

}
