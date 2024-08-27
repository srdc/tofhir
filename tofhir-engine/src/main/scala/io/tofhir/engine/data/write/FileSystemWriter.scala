package io.tofhir.engine.data.write

import com.typesafe.scalalogging.Logger
import io.tofhir.common.model.Json4sSupport.formats
import io.tofhir.engine.data.write.FileSystemWriter.SinkFileFormats
import io.tofhir.engine.model.{FhirMappingResult, FileSystemSinkSettings}
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.util.CollectionAccumulator
import org.json4s.jackson.JsonMethods

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
      // Handle cases where the file format is either NDJSON, PARQUET, or DELTA_LAKE,
      // and the output needs to be partitioned by FHIR resource type.
      case SinkFileFormats.NDJSON | SinkFileFormats.PARQUET | SinkFileFormats.DELTA_LAKE if sinkSettings.partitionByResourceType =>
        // Transform the DataFrame (df) to include the resource type.
        // parsedDF will have the following structure:
        // +------------+-------------------------+
        // | resourceType | mappedResourceJson    |
        // +------------+-------------------------+
        // | Patient      | {"resourceType": "... |
        // | Patient      | {"resourceType": "... |
        // +------------+-------------------------+
        val parsedDF = df
          .map(row => {
            // extract `mappedResource` and parse the resourceType from the JSON.
            val mappedResourceJson = row.mappedResource.get
            val resourceType: String = (JsonMethods.parse(mappedResourceJson) \ "resourceType").extract[String]
            (resourceType, mappedResourceJson)
          })
          .toDF("resourceType", "mappedResourceJson")

        // Group the DataFrame by resourceType to aggregate all resources of the same type.
        // groupedDFs will have the following structure:
        // +------------+--------------------+
        // | resourceType | resources     |
        // +------------+--------------------+
        // | Patient |[ {"resourceType":...|
        // +------------+--------------------+
        val groupedDFs = parsedDF.groupBy("resourceType").agg(collect_list("mappedResourceJson").as("resources"))

        // Iterate through each group (by resourceType) and write the data to separate folders.
        groupedDFs.collect().foreach(rDf => {
          // Extract the resourceType for the current group.
          val resourceType = rDf.getAs[String]("resourceType")
          // Convert the mutable ArraySeq (default in Spark) to an immutable List
          val resourcesSeq = rDf.getAs[Seq[String]]("resources").toList
          // Convert the list of resources back into a DataFrame with a single column named "mappedResourceJson".
          // This DataFrame will look like:
          // +----------------------+
          // | mappedResourceJson   |
          // +----------------------+
          // | {"resourceType": "...|
          // | {"resourceType": "...|
          // +----------------------+
          val resourcesDF = resourcesSeq.toDF("mappedResourceJson")

          // Define the output path based on the resourceType, ensuring that each resource type is saved in its own folder.
          val outputPath = s"${sinkSettings.path}/$resourceType"
          // Write the resources to the specified path based on the chosen format.
          val writer = resourcesDF
            .coalesce(sinkSettings.numOfPartitions)
            .write
            .mode(SaveMode.Append)
            .options(sinkSettings.options)

          // Handle the specific formats
          sinkSettings.sinkType match {
            case SinkFileFormats.NDJSON => writer.text(outputPath)
            case SinkFileFormats.PARQUET => writer.parquet(outputPath)
            case SinkFileFormats.DELTA_LAKE => writer.format(SinkFileFormats.DELTA_LAKE).save(outputPath)
          }
        })
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
      case SinkFileFormats.DELTA_LAKE =>
        val writer =
          df
            .map(_.mappedResource.get)
            .coalesce(sinkSettings.numOfPartitions)
            .write
            .format(SinkFileFormats.DELTA_LAKE) // Specify Delta Lake format
            .mode(SaveMode.Append)
            .options(sinkSettings.options)
        writer.save(sinkSettings.path)
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
    final val DELTA_LAKE = "delta"
  }
}
