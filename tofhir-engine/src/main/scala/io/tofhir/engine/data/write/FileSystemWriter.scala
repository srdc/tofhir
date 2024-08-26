package io.tofhir.engine.data.write

import com.typesafe.scalalogging.Logger
import io.tofhir.engine.data.write.FileSystemWriter.SinkFileFormats
import io.tofhir.engine.model.{FhirMappingResult, FileSystemSinkSettings}
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.util.CollectionAccumulator

class FileSystemWriter(sinkSettings: FileSystemSinkSettings) extends BaseFhirWriter(sinkSettings) {
  private val logger: Logger = Logger(this.getClass)
  /**
   * Write the data frame of json serialized FHIR resources to given sink (e.g. FHIR repository)
   *
   * @param df Dataframe of serialized jsons
   */
  override def write(spark:SparkSession, df: Dataset[FhirMappingResult], problemsAccumulator:CollectionAccumulator[FhirMappingResult]): Unit = {
    import spark.implicits._
    logger.debug("Created FHIR resources will be written to the given URL:{}", sinkSettings.path)
    sinkSettings.sinkType match {
      case SinkFileFormats.NDJSON =>
        val writer =
          df
            .map(_.mappedResource.get)
            .coalesce(sinkSettings.numOfPartitions)
            .write
            .mode(SaveMode.Append)
            .options(sinkSettings.options)
        writer.text(sinkSettings.path)
      case SinkFileFormats.PARQUET =>
        val writer =
          df
            .map(_.mappedResource.get)
            .coalesce(sinkSettings.numOfPartitions)
            .write
            .mode(SaveMode.Append)
            .options(sinkSettings.options)
        writer.parquet(sinkSettings.path)
      case SinkFileFormats.CSV =>
        // read the mapped resource json column and load it to a new data frame
        val mappedResourceDF = spark.read.json(df.select("mappedResource").as[String])
        // select the columns that are not array type or struct type
        // since the CSV is a flat data structure
        val nonArrayAndStructCols = mappedResourceDF.schema.fields.filterNot(field => {
          field.dataType.isInstanceOf[ArrayType] || field.dataType.isInstanceOf[StructType]
        }).map(_.name)
        // if the DataFrame contains data, write it to the specified path
        if(!mappedResourceDF.isEmpty){
          val filteredDF = mappedResourceDF.select(nonArrayAndStructCols.head, nonArrayAndStructCols.tail: _*)
          // write the mapped resources to a CSV file
          filteredDF
            .coalesce(sinkSettings.numOfPartitions)
            .write
            .mode(SaveMode.Append)
            .options(sinkSettings.options)
            .csv(sinkSettings.path)
        }
      case _ =>
        throw new NotImplementedError()
    }
  }

  /**
   * Validates the current FHIR writer.
   *
   * For the FileSystemWriter, validation is not implemented and this method does nothing.
   */
  override def validate(): Unit = {}
}

object FileSystemWriter {
  object SinkFileFormats {
    final val NDJSON = "ndjson"
    final val CSV = "csv"
    final val PARQUET = "parquet"
  }
}
