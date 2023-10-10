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
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.util

/**
 * Handler for post mapping processing. Spark is used to process the data sources.
 */
object PostMappingHandler {

  /**
   * Starting point, two functionalities are implemented:
   * 1. Save erroneous data sources row by row based on error types (e.g. invalid input, mapping error, invalid resource)
   * 2. Archive data sources as a whole based on archiving mode (off, archive, delete)
   * @param spark
   * @param df
   * @param mappingJobExecution
   * @param mappingUrl
   * @param notWrittenResources
   * @param mappingErrors
   * @param invalidInputs
   */
  def saveErroneousRecordsAndArchiveDataSource(spark: SparkSession,
                                               df: Dataset[FhirMappingResult],
                                               mappingJobExecution: FhirMappingJobExecution,
                                               mappingUrl: Option[String],
                                               notWrittenResources: util.List[FhirMappingResult],
                                               mappingErrors: Dataset[FhirMappingResult],
                                               invalidInputs: Dataset[FhirMappingResult]): Unit = {
    saveErroneousRecords(spark, mappingJobExecution, mappingUrl, notWrittenResources, mappingErrors, invalidInputs)
    archiveDataSource(df, mappingJobExecution)
  }

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
  def saveErroneousRecords(spark: SparkSession,
                           mappingJobExecution: FhirMappingJobExecution,
                           mappingUrl: Option[String],
                           notWrittenResources: util.List[FhirMappingResult],
                           mappingErrors: Dataset[FhirMappingResult],
                           invalidInputs: Dataset[FhirMappingResult]): Unit = {
    if (mappingJobExecution.job.dataProcessingSettings.saveErroneousRecords) {
      if (!invalidInputs.isEmpty) {
        this.writeErroneousDataset(mappingJobExecution, invalidInputs, mappingUrl.get, FhirMappingErrorCodes.INVALID_INPUT)
      }
      if (!mappingErrors.isEmpty) {
        this.writeErroneousDataset(mappingJobExecution, mappingErrors, mappingUrl.get, FhirMappingErrorCodes.MAPPING_ERROR)
      }
      if (!notWrittenResources.isEmpty) {
        import spark.implicits._
        val notWrittenResourcesDs = spark.createDataset[FhirMappingResult](notWrittenResources)
        this.writeErroneousDataset(mappingJobExecution, notWrittenResourcesDs, mappingUrl.get, FhirMappingErrorCodes.INVALID_RESOURCE)
      }
    }
  }

  /**
   * Writes the dataset to the errorOutputDirectory. Directory structure:
   * error-folder-path\<error-type>\job-<jobId>\execution-<executionId>\<mappingUrl>\<random-generated-name-by-spark>.csv
   *
   * @param mappingJobExecution job execution to get output directory of data sources with errors
   * @param dataset             filtered dataset of data sources with errors to write to configured folder
   * @param mappingUrl          to create directory for each mapping url within the job execution
   * @param errorType           one of invalid_input, mapping_error, invalid_resource
   */
  private def writeErroneousDataset(mappingJobExecution: FhirMappingJobExecution,
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
   * Start archiving operation for the given job execution and dataset
   * Archiving is only applied if data source type is file system and archive mode is not off
   * @param df
   * @param mappingJobExecution
   */
  private def archiveDataSource(df: Dataset[FhirMappingResult], mappingJobExecution: FhirMappingJobExecution): Unit = {
    // check if archive mode is enabled for that job
    mappingJobExecution.job.dataProcessingSettings.archiveMode match {
      case ArchiveModes.OFF => // do nothing
      case _ =>
        mappingJobExecution.job.sourceSettings.head._2 match {
          case fileSystemSourceSettings: FileSystemSourceSettings => this.applyArchiving(df, mappingJobExecution)
          case _ => // do nothing, we only support file system source settings for archiving
        }
    }
  }

  /**
   * Apply archiving for file type data sources e.g. move to archive folder, delete from source folder
   * Dataset structure is different for streaming and batch jobs, so we need to handle them differently
   * Streaming: input file path for each row is included in the dataframe, so we can group by filename and move/delete them
   * Batch: input file path is not included in the dataframe, so we need to get it from the job execution and move/delete them
   * @param df
   * @param mappingJobExecution
   */
  private def applyArchiving(df: Dataset[FhirMappingResult], mappingJobExecution: FhirMappingJobExecution): Unit = {
    val fileSystemSourceSettings = mappingJobExecution.job.sourceSettings.head._2.asInstanceOf[FileSystemSourceSettings]
    // get data folder path from data source settings
    val dataFolderPath = fileSystemSourceSettings.dataFolderPath
    if (fileSystemSourceSettings.asStream) {
      // get schema from first row to be used for json parsing of 'source' column in df
      val schema = schema_of_json(df.collect().head.source.get)
      // uri paths of input source files are included in the dataframes for the streaming jobs, get by grouping by filename
      df
        // go one level deeper in the df
        .withColumn("jsonData", from_json(col("source"), schema))
        .select("jsonData.*")
        // group by filename to get unique filenames
        .groupBy(col("filename"))
        .agg(col("filename"))
        // for each filename, process file operations
        .foreach(x => {
          val absoluteFilePaths = x.getString(0)
          // create path object for each file path
          val absolutePath = Paths.get(new URI(absoluteFilePaths))
          // create a relative path to the context path of the engine for that file
          val relativeFilePath = FileUtils.getPath("").toAbsolutePath.relativize(absolutePath)
          this.processFileOperationsByArchiveMode(mappingJobExecution, relativeFilePath)
        })
    } else {
      // find specific file path by matching mapping urls in the job and df
      df.groupBy("mappingUrl").agg(col("mappingUrl")) // df has multiple rows with same mapping urls, group by to get distinct mapping urls
        .foreach { row =>
          val mappingUrl = row.getString(0)
          val paths = mappingJobExecution.job.mappings.find(x => x.mappingRef == mappingUrl).get.sourceContext.map(fhirMappingSourceContextMap => {
            fhirMappingSourceContextMap._2 match {
              case fileSystemSource: FileSystemSource => fileSystemSource.path
            }
          })
          paths.foreach(relativePath => {
            val relativeFilePath = Paths.get(dataFolderPath, relativePath)
            this.processFileOperationsByArchiveMode(mappingJobExecution, relativeFilePath)
          })
        }
    }
  }

  /**
   * Process file operations by archive mode of execution
   * If archive mode is delete, delete file from source folder
   * If archive mode is archive, move file to path
   * @param mappingJobExecution
   * @param relativeFilePath
   */
  private def processFileOperationsByArchiveMode(mappingJobExecution: FhirMappingJobExecution, relativeFilePath: Path): Unit = {
    mappingJobExecution.job.dataProcessingSettings.archiveMode match {
      case ArchiveModes.DELETE => this.deleteSourceFile(relativeFilePath)
      case ArchiveModes.ARCHIVE =>
        val archivePath = FileUtils.getPath(ToFhirConfig.engineConfig.archiveFolder, relativeFilePath.toString)
        this.moveSourceFileToArchive(relativeFilePath, archivePath)
    }
  }

  /**
   * Delete file from given path
   * @param path
   */
  private def deleteSourceFile(path: Path): Unit = {
    val file = new File(path.toString)
    file.delete()
  }

  /**
   * Move file from given path to given archive path
   * @param path
   * @param archivePath
   */
  private def moveSourceFileToArchive(path: Path, archivePath: Path): Unit = {
    val archiveFile = new File(archivePath.toString)
    // create parent directories if not exists
    archiveFile.getParentFile.mkdirs()
    // move file to archive folder
    Files.move(path, archiveFile.toPath, StandardCopyOption.ATOMIC_MOVE)
  }

}
