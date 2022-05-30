package io.onfhir.tofhir.data.read

import io.onfhir.tofhir.model.{FileSystemSource, SourceFileFormats}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.file.Paths

/**
 * Reader from file system
 * @param spark           Spark session
 * @param sourceSettings  File system source settings
 */
class FileDataSourceReader(spark:SparkSession) extends BaseDataSourceReader[FileSystemSource]{

  /**
   * Read the source data for the given task
   *
   * @param mappingSource
   * @param schema
   * @return
   */
  override def read(mappingSource: FileSystemSource, schema:StructType): DataFrame = {
    val finalPath = generateFilePath(mappingSource)
    mappingSource.sourceType match {
      case SourceFileFormats.CSV =>
        spark.read
          .option("header", true) //TODO make this optional
          .option("inferSchema", false)
          .option("enforceSchema", true)  //Enforce the given schema
          .schema(schema)
          .csv(finalPath)
      case SourceFileFormats.JSON => spark.read.schema(schema).json(finalPath)
      case SourceFileFormats.PARQUET => spark.read.schema(schema).parquet(finalPath)
      case _ => throw new NotImplementedError()
    }
  }

  /**
   * Read the source data for the given task
   *
   * @param mappingSource
   * @return
   */
  override def read(mappingSource: FileSystemSource): DataFrame = {
    val finalPath = generateFilePath(mappingSource)
    mappingSource.sourceType match {
      case SourceFileFormats.CSV =>
        spark.read
          .option("header", true)
          .option("inferSchema", true)
          .csv(finalPath)
      case SourceFileFormats.JSON => spark.read.json(finalPath)
      case SourceFileFormats.PARQUET => spark.read.parquet(finalPath)
      case _ => throw new NotImplementedError()
    }
  }

  def generateFilePath(mappingSource: FileSystemSource): String = {
    val dataFolderPath = Paths.get(mappingSource.settings.dataFolderPath).normalize().toString
    val mappingFilePath = Paths.get(mappingSource.path).normalize().toString
    Paths.get(dataFolderPath, mappingFilePath).toAbsolutePath.toString
  }

}
