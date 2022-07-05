package io.onfhir.tofhir.data.read

import com.typesafe.scalalogging.Logger
import io.onfhir.tofhir.model.{FileSystemSource, SourceFileFormats}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.file.Paths
import java.time.LocalDateTime

/**
 * Reader from file system
 *
 * @param spark Spark session
 */
class FileDataSourceReader(spark: SparkSession) extends BaseDataSourceReader[FileSystemSource] {

  private val logger: Logger = Logger(this.getClass)

  /**
   * Read the source data
   *
   * @param mappingSource Context/configuration information for mapping source
   * @param schema        Optional schema for the source
   * @return
   */
  override def read(mappingSource: FileSystemSource, schema: Option[StructType], timeRange: Option[(LocalDateTime, LocalDateTime)]): DataFrame = {
    val dataFolderPath = Paths.get(mappingSource.settings.dataFolderPath).normalize().toString
    val mappingFilePath = Paths.get(mappingSource.path).normalize().toString
    val finalPath = Paths.get(dataFolderPath, mappingFilePath).toAbsolutePath.toString

    var inferSchema = false
    var enforceSchema = true
    if(schema.isEmpty) {
      logger.warn("I would like to have a schema definition while reading data from the file system. But you did not provide, hence I will try to infer the schema and read data accordingly.")
      inferSchema = true
      enforceSchema = false
    }

    mappingSource.sourceType match {
      case SourceFileFormats.CSV =>
        spark.read
          .option("header", true) //TODO make this optional
          .option("inferSchema", inferSchema)
          .option("enforceSchema", enforceSchema) //Enforce the given schema
          .schema(schema.orNull)
          .csv(finalPath)
      case SourceFileFormats.JSON => spark.read.schema(schema.orNull).json(finalPath)
      case SourceFileFormats.PARQUET => spark.read.schema(schema.orNull).parquet(finalPath)
      case _ => throw new NotImplementedError()
    }
  }
}
