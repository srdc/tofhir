package io.tofhir.engine.data.write

import com.typesafe.scalalogging.Logger
import FileSystemWriter.SinkFileFormats
import io.tofhir.engine.model.{FhirMappingResult, FileSystemSinkSettings}
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
      case _ =>
        throw new NotImplementedError()
    }
  }
}

object FileSystemWriter {
  object SinkFileFormats {
    final val NDJSON = "ndjson"
  }
}
