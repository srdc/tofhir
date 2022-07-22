package io.onfhir.tofhir.data.write

import com.typesafe.scalalogging.Logger
import io.onfhir.tofhir.data.write.FileSystemWriter.SinkFileFormats
import io.onfhir.tofhir.model.FileSystemSinkSettings
import org.apache.spark.sql.{Dataset, SaveMode}

class FileSystemWriter(sinkSettings: FileSystemSinkSettings) extends BaseFhirWriter(sinkSettings) {
  private val logger: Logger = Logger(this.getClass)
  /**
   * Write the data frame of json serialized FHIR resources to given sink (e.g. FHIR repository)
   *
   * @param df Dataframe of serialized jsons
   */
  override def write(df: Dataset[String]): Unit = {
    logger.debug("Created FHIR resources will be written to the given URL:{}", sinkSettings.path)
    sinkSettings.sinkType match {
      case SinkFileFormats.NDJSON =>
        val writer =
          df
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
