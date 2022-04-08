package io.onfhir.tofhir.engine

import io.onfhir.tofhir.model.{FhirMappingFromFileSystemTask, FileSystemSourceSettings, SourceFileFormats}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.file.Paths

/**
 * Reader from file system
 * @param spark           Spark session
 * @param sourceSettings  File system source settings
 */
class FileDataSourceReader(spark:SparkSession, sourceSettings:FileSystemSourceSettings) extends BaseDataSourceReader[FhirMappingFromFileSystemTask](sourceSettings) {

  /**
   * Read the source data for the given task
   *
   * @param mappingTask
   * @return
   */
  override def read(mappingTask: FhirMappingFromFileSystemTask): DataFrame = {
    val dataFolderPath = Paths.get(sourceSettings.dataFolderUri).normalize().toString
    val mappingFilePath = Paths.get(mappingTask.path).normalize().toString
    val finalPath = Paths.get(dataFolderPath, mappingFilePath).toAbsolutePath.toString
    mappingTask.sourceType match {
      case SourceFileFormats.CSV => spark.read.option("header", "true").csv(finalPath)
      case SourceFileFormats.JSON => spark.read.json(finalPath)
      case SourceFileFormats.PARQUET => spark.read.parquet(finalPath)
      case _ => throw new NotImplementedError()
    }
  }
}
