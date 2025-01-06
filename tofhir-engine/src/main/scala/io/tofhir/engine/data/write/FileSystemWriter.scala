package io.tofhir.engine.data.write

import com.typesafe.scalalogging.Logger
import io.tofhir.engine.data.write.FileSystemWriter.SinkContentTypes
import io.tofhir.engine.model.{FhirMappingResult, FileSystemSinkSettings}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem}
import org.apache.spark.sql.functions.{col, collect_list}
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.{DataFrameWriter, Dataset, SaveMode, SparkSession}
import org.apache.spark.util.CollectionAccumulator

import java.net.URI

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
    sinkSettings.contentType match {
      // Handle cases where the content type is either NDJSON, PARQUET, or DELTA_LAKE,
      // and the output needs to be partitioned by FHIR resource type.
      case SinkContentTypes.NDJSON | SinkContentTypes.PARQUET | SinkContentTypes.DELTA_LAKE if sinkSettings.partitionByResourceType =>
        // Check if the sink path is for HDFS
        if (sinkSettings.path.startsWith("hdfs://")) {
          // Extract the scheme (e.g., "hdfs") and authority (e.g., "namenode:9000") from the sinkSettings.path
          val uri = new URI(sinkSettings.path)
          val defaultFS = s"${uri.getScheme}://${uri.getAuthority}"

          // Process each partition of the DataFrame
          df.foreachPartition((partition: Iterator[FhirMappingResult]) => {
            // Collect all elements in the current partition into a sequence
            val fhirMappingResults: Seq[FhirMappingResult] = partition.toSeq
            // Check if the partition contains any records
            if (fhirMappingResults.nonEmpty) {
              // Group the records by their resourceType
              fhirMappingResults.groupBy(_.resourceType.get).foreach { case (resourceType, fhirResources) =>
                // Log the number of resources and the resourceType being written
                logger.debug("Will write {} {} resources to HDFS.", fhirResources.length, resourceType)
                // Prepare the data to be written by concatenating resources into a single string, separated by newlines
                val data = fhirResources.map(_.mappedFhirResource.get.mappedResource.get).mkString("\n")

                // Initialize Hadoop Configuration
                val conf = new Configuration()
                conf.set("fs.defaultFS", defaultFS) // Set the HDFS default file system dynamically
                // Get the HDFS FileSystem instance using the configuration
                val fs = FileSystem.get(conf)

                // Define the HDFS directory path for the current resourceType
                val dirPath = new org.apache.hadoop.fs.Path(s"${sinkSettings.path}/$resourceType")
                // Generate a unique file name to avoid conflicts when multiple files are being written concurrently
                val uniqueFilePath = new org.apache.hadoop.fs.Path(dirPath, s"${java.util.UUID.randomUUID().toString}.txt")

                var outputStream: FSDataOutputStream = null
                try {
                  // Ensure the directory exists; create it if it does not
                  if (!fs.exists(dirPath)) {
                    fs.mkdirs(dirPath)
                  }
                  // Open an output stream for the unique file path
                  outputStream = fs.create(uniqueFilePath, true)
                  // Write the prepared data to the output stream
                  outputStream.writeBytes(data)
                  // Log success message
                  logger.info(s"Successfully wrote data to $uniqueFilePath")
                } catch {
                  case e: Exception =>
                    logger.error(s"Failed to write data to $uniqueFilePath: ${e.getMessage}", e)
                    throw e
                } finally {
                  // Close the output stream to ensure data is flushed and resources are released
                  if (outputStream != null) {
                    try {
                      outputStream.close()
                    } catch {
                      case e: Exception =>
                        logger.error(s"Failed to close output stream for $uniqueFilePath: ${e.getMessage}", e)
                        throw e
                    }
                  }
                }
              }
            }
          })
        } else {
          // Group the DataFrame by resourceType to aggregate all resources of the same type.
          // groupedDFs will have the following structure:
          // +------------+--------------------+
          // | resourceType | resources     |
          // +------------+--------------------+
          // | Patient |[ {"resourceType":...|
          // +------------+--------------------+
          val groupedDFs = df.groupBy("resourceType").agg(collect_list("mappedFhirResource.mappedResource").as("resources"))
          // Iterate through each group (by resourceType) and write the data to separate folders.
          groupedDFs.collect().foreach(rDf => {
            // Extract the resourceType for the current group.
            val resourceType = rDf.getAs[String]("resourceType")
            // Convert the mutable ArraySeq (default in Spark) to an immutable List
            val resourcesSeq = rDf.getAs[Seq[String]]("resources").toList
            // Get the partition columns for the given resourceType
            val partitionColumns = sinkSettings.getPartitioningColumns(resourceType)

            // Generate the DataFrame that will be written to the file system.
            // If the sink type is NDJSON, the DataFrame should have a single column containing the JSON strings.
            // For other content types, the DataFrame should have multiple columns corresponding to the keys in the JSON objects.
            val resourcesDF = if (sinkSettings.contentType.contentEquals(SinkContentTypes.NDJSON)) {
              // Convert the list of JSON strings into a DataFrame with a single column named "mappedResourceJson".
              // The resulting DataFrame will contain one row per JSON string, where each row is a single JSON object.
              // The structure of this DataFrame will be as follows:
              // +----------------------+
              // | mappedResourceJson   |
              // +----------------------+
              // | {"resourceType": "...|
              // | {"resourceType": "...|
              // +----------------------+
              resourcesSeq.toDF("mappedResourceJson")
            } else {
              // Convert the list of JSON strings into a Dataset[String], where each element is a JSON string.
              val resourcesDS = spark.createDataset(resourcesSeq)
              // Create a DataFrame by reading the Dataset of JSON strings as JSON objects.
              // The resulting DataFrame will have multiple columns based on the keys in the JSON strings.
              // Each JSON object will be represented as a row in the DataFrame, with the columns corresponding to the JSON keys.
              // For example, the DataFrame might look like this:
              // +-----------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+------------+--------------------+--------------------+
              // | abatementDateTime | asserter | category | clinicalStatus | code | encounter | id | meta | onsetDateTime | resourceType | subject | verificationStatus |
              // +-----------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+------------+--------------------+--------------------+
              // | NULL | NULL | [ {problem-list-item} ] | {active, http: //...| {J13, Pneumonia} | NULL | 2faab6373e7c3bba4...| [ {https://aiccele...| 2012-10-15 | Condition | {Patient/34dc88d5... | {confirmed, http... |
              // | 2013-05-22 | NULL | [ {encounter-diagnosis} ] | {inactive, http...| {G40, Parkinson's disease} | Encounter/bb7134...| 63058b87a718e66d4...| [ {https://aiccele...| 2013-05-07 | Condition | {Patient/0b3a0b23... | NULL |
              // +-----------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+------------+--------------------+--------------------+
              val resourcesDF = spark.read.json(resourcesDS)

              // Extend resourcesDF to handle partitioning if required.
              // Some partition columns may not exist in the DataFrame (e.g., nested fields like `subject.reference`),
              // so this extension adds the missing columns, allowing Spark to partition the DataFrame accordingly.
              if (partitionColumns.isEmpty) {
                // If no partition columns are defined, return the DataFrame as-is
                resourcesDF
              } else {
                // Obtain all existing columns
                val existingColumns = resourcesDF.columns
                // Filter out partition columns that are already in existingColumns
                val filteredPartitionColumns = partitionColumns.filterNot(pc => existingColumns.exists(_.contentEquals(pc)))
                // Merge existingColumns with the filtered partition columns
                // This ensures that partition columns, which may not be part of the original data, are included.
                val allColumnsWithPartition = existingColumns.map(col) ++ filteredPartitionColumns.map(c => col(c).as(c))

                // Create a new DataFrame by selecting all existing columns along with the added partition columns
                resourcesDF
                  .select(allColumnsWithPartition: _*)
              }
            }

            // Define the output path based on the resourceType, ensuring that each resource type is saved in its own folder.
            val outputPath = s"${sinkSettings.path}/$resourceType"
            // Write the resources to the specified path based on the chosen content type.
            val writer = getWriter(resourcesDF, sinkSettings)

            // Apply partitioning if partition columns are specified
            val partitionedWriter = if (partitionColumns.nonEmpty) {
              writer.partitionBy(partitionColumns: _*)
            } else {
              writer
            }

            // Handle the specific content types
            sinkSettings.contentType match {
              case SinkContentTypes.NDJSON => partitionedWriter.text(outputPath)
              case SinkContentTypes.PARQUET => partitionedWriter.parquet(outputPath)
              case SinkContentTypes.DELTA_LAKE => partitionedWriter.format(SinkContentTypes.DELTA_LAKE).save(outputPath)
            }
          })
        }
      case SinkContentTypes.NDJSON =>
        getWriter(df.map(_.mappedFhirResource.get.mappedResource.get), sinkSettings).text(sinkSettings.path)
      case SinkContentTypes.PARQUET =>
        // Convert the DataFrame to a Dataset of JSON strings
        val jsonDS = df.select("mappedFhirResource.mappedResource").as[String]
        // Create a DataFrame from the Dataset of JSON strings
        val jsonDF = spark.read.json(jsonDS)
        getWriter(jsonDF, sinkSettings).parquet(sinkSettings.path)
      case SinkContentTypes.DELTA_LAKE =>
        // Convert the DataFrame to a Dataset of JSON strings
        val jsonDS = df.select("mappedFhirResource.mappedResource").as[String]
        // Create a DataFrame from the Dataset of JSON strings
        val jsonDF = spark.read.json(jsonDS)
        getWriter(jsonDF, sinkSettings)
          .format(SinkContentTypes.DELTA_LAKE) // Specify Delta Lake content type
          .save(sinkSettings.path)
      case SinkContentTypes.CSV =>
        // read the mapped resource json column and load it to a new data frame
        val mappedResourceDF = spark.read.json(df.select("mappedFhirResource.mappedResource").as[String])
        // select the columns that are not array type or struct type
        // since the CSV is a flat data structure
        val nonArrayAndStructCols = mappedResourceDF.schema.fields.filterNot(field => {
          field.dataType.isInstanceOf[ArrayType] || field.dataType.isInstanceOf[StructType]
        }).map(_.name)
        // if the DataFrame contains data, write it to the specified path
        if (!mappedResourceDF.isEmpty) {
          val filteredDF = mappedResourceDF.select(nonArrayAndStructCols.head, nonArrayAndStructCols.tail: _*)
          // write the mapped resources to a CSV file
          getWriter(filteredDF, sinkSettings).csv(sinkSettings.path)
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

  /**
   * Creates a configured DataFrameWriter for a given Dataset based on the provided sink settings.
   *
   * @param dataset      The Dataset to be written. The Dataset can be of any type `T`.
   * @param sinkSettings The settings used to configure the DataFrameWriter, including
   *                     the number of partitions, write mode, and options.
   * @tparam T The type of the Dataset elements.
   * @return A DataFrameWriter[T] configured according to the provided sink settings.
   */
  private def getWriter[T](dataset: Dataset[T], sinkSettings: FileSystemSinkSettings): DataFrameWriter[T] = {
    dataset
      .coalesce(sinkSettings.numOfPartitions)
      .write
      .mode(SaveMode.Append)
      .options(sinkSettings.options)
  }
}

object FileSystemWriter {
  object SinkContentTypes {
    final val NDJSON = "ndjson"
    final val CSV = "csv"
    final val PARQUET = "parquet"
    final val DELTA_LAKE = "delta"
  }
}
