package io.onfhir.tofhir.data.read

import io.onfhir.tofhir.model.{FileSystemSource, FileSystemSourceSettings, SourceFileFormats}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.file.Paths
import java.time.LocalDateTime

/**
 * Reader from file system
 *
 * @param spark Spark session
 */
class FileDataSourceReader(spark: SparkSession) extends BaseDataSourceReader[FileSystemSource, FileSystemSourceSettings] {

  /**
   * Read the source data
   *
   * @param mappingSource Context/configuration information for mapping source
   * @param schema        Optional schema for the source
   * @return
   */
  override def read(mappingSource: FileSystemSource, sourceSettings:FileSystemSourceSettings, schema: Option[StructType], timeRange: Option[(LocalDateTime, LocalDateTime)]): DataFrame = {
    //Construct final path to the folder or file
    val dataFolderPath = Paths.get(sourceSettings.dataFolderPath).normalize().toString
    val mappingFilePath = Paths.get(mappingSource.path).normalize().toString
    val finalPath = Paths.get(dataFolderPath, mappingFilePath).toAbsolutePath.toString

    //Based on source type
    mappingSource.sourceType match {
        case SourceFileFormats.CSV =>
          //Options that we infer for csv
          val inferSchema = schema.isEmpty
          val enforceSchema = schema.isDefined
          val includeHeader = mappingSource.options.get("header").forall(_ == "true")

          //Other options
          val otherOptions = mappingSource.options.filterNot(o => o._1 == "header" || o._1 == "inferSchema" || o._1 == "enforceSchema")
          if(sourceSettings.asStream)
            spark.readStream
              .option("header", includeHeader)
              .option("inferSchema", inferSchema)
              .option("enforceSchema", enforceSchema) //Enforce the given schema
              .options(otherOptions)
              .schema(schema.orNull)
              .csv(finalPath)
          else
            spark.read
              .option("header", includeHeader)
              .option("inferSchema", inferSchema)
              .option("enforceSchema", enforceSchema) //Enforce the given schema
              .options(otherOptions)
              .schema(schema.orNull)
              .csv(finalPath)
        case SourceFileFormats.JSON =>
          if(sourceSettings.asStream)
            spark.readStream.options(mappingSource.options).schema(schema.orNull).json(finalPath)
          else
            spark.read.options(mappingSource.options).schema(schema.orNull).json(finalPath)
        case SourceFileFormats.PARQUET =>
          if(sourceSettings.asStream)
            spark.readStream.options(mappingSource.options).schema(schema.orNull).parquet(finalPath)
          else
            spark.read.options(mappingSource.options).schema(schema.orNull).parquet(finalPath)
        case _ => throw new NotImplementedError()
      }
  }
}
