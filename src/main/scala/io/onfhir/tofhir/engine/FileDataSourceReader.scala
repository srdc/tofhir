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
    val finalPath = Paths.get(sourceSettings.dataFolderPath, mappingTask.path).toString
    mappingTask.sourceType match {
      case SourceFileFormats.CSV => spark.read.csv(finalPath)
      case SourceFileFormats.JSON => spark.read.json(finalPath)
      case SourceFileFormats.PARQUET => spark.read.parquet(finalPath)
      case _ => throw new NotImplementedError()
    }
  }
}
