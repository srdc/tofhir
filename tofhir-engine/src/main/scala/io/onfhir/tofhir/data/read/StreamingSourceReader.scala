package io.onfhir.tofhir.data.read

import com.typesafe.scalalogging.Logger
import io.onfhir.tofhir.model.StreamingSource
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

/**
 *
 * @param spark
 */
class StreamingSourceReader(spark: SparkSession) extends BaseDataSourceReader[StreamingSource] {

  private val logger: Logger = Logger(this.getClass)

  /**
   * Read the source data for the given task
   *
   * @param mappingSource Context/configuration information for mapping source
   * @param schema        Schema for the source
   * @return
   */
  override def read(mappingSource: StreamingSource, schema: Option[StructType] = Option.empty, timeRange: Option[(LocalDateTime, LocalDateTime)] = Option.empty): DataFrame = {
    import spark.implicits._

    if(schema.isEmpty) {
      throw new IllegalArgumentException("Schema is required for streaming source")
    }

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", mappingSource.settings.bootstrapServers)
      .option("subscribe", mappingSource.topicName)
      .option("startingOffsets", mappingSource.startingOffsets)
      .option("inferSchema", true)
      .load()
      .select(from_json($"value".cast(StringType), schema.get).as("record"))
      .select("record.*")
    df

  }
}
