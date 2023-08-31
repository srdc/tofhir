package io.tofhir.engine.data.read

import com.fasterxml.jackson.core.JsonParseException
import io.onfhir.api.Resource
import io.onfhir.util.JsonFormatter._
import io.tofhir.engine.model.{KafkaSource, KafkaSourceSettings}
import org.apache.spark.sql.functions.{col, from_json, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.{JBool, JDouble, JInt, JLong, JNull}

import java.time.LocalDateTime
import javax.ws.rs.InternalServerErrorException

/**
 *
 * @param spark
 */
class KafkaSourceReader(spark: SparkSession) extends BaseDataSourceReader[KafkaSource, KafkaSourceSettings] {

  /**
   * Read the source data for the given task
   *
   * @param mappingSource Context/configuration information for mapping source
   * @param schema        Schema for the source
   * @param limit         Limit the number of rows to read
   * @param jobId         The identifier of mapping job which executes the mapping
   * @return
   */
  override def read(mappingSource: KafkaSource, sourceSettings: KafkaSourceSettings, schema: Option[StructType] = Option.empty, timeRange: Option[(LocalDateTime, LocalDateTime)] = Option.empty, limit: Option[Int] = Option.empty, jobId: Option[String] = Option.empty): DataFrame = {
    import spark.implicits._

    if (schema.isEmpty) {
      throw new IllegalArgumentException("Schema is required for streaming source")
    }

    // user-defined function (UDF) to process message (json) coming from the Kafka topic
    val processDataUDF = udf((message: String) => {
      val json: Resource =
        try { // try-catch block needed to handle unparseable json
          message.parseJson
        } catch {
          case e: JsonParseException => throw new InternalServerErrorException("Kafka message is an unparseable JSON", e)
        }

      // process each field so that string values are acceptable (if they are parseable of course) for some data types such as double and integer
      json.mapField(field => {
        schema.get.fields.find(p => p.name.contentEquals(field._1)).map(fieldType => { // get field type from the schema
          fieldType.dataType match {
            case _: DoubleType =>
              try {
                field._1 -> {
                  // performs the following conversions:
                  //  JString(v) => JDouble(v)
                  //  JDouble(v) => JDouble(v)
                  //  JString() => JNull
                  field._2.extract[String] match {
                    case str if str.nonEmpty => JDouble(str.toDouble)
                    case _ => JNull
                  }
                }
              } catch {
                case e: NumberFormatException => throw new InternalServerErrorException(s"${field._2} is not a parsable `Double`", e)
              }
            case _: IntegerType =>
              try {
                field._1 -> {
                  // performs the following conversions:
                  //  JString(v) => JInt(v)
                  //  JInt(v) => JInt(v)
                  //  JString() => JNull
                  field._2.extract[String] match {
                    case str if str.nonEmpty => JInt(str.toInt)
                    case _ => JNull
                  }
                }
              } catch {
                case e: NumberFormatException => throw new InternalServerErrorException(s"${field._2} is not a parsable `Integer`", e)
              }
            case _: LongType =>
              try {
                field._1 -> {
                  // performs the following conversions:
                  //  JString(v) => JLong(v)
                  //  JLong(v) => JLong(v)
                  //  JString() => JNull
                  field._2.extract[String] match {
                    case str if str.nonEmpty => JLong(str.toLong)
                    case _ => JNull
                  }
                }
              } catch {
                case e: NumberFormatException => throw new InternalServerErrorException(s"${field._2} is not a parsable `Long`", e)
              }
            case _: BooleanType =>
              try {
                field._1 -> {
                  // performs the following conversions:
                  //  JString(v) => JBool(v)
                  //  JBool(v) => JBool(v)
                  //  JString() => JNull
                  val bool = field._2.extractOpt[Boolean] // try to extract as boolean
                  bool match {
                    // matches JBool(v)
                    case Some(value) => JBool(value)
                    // try to extract as string
                    case None => field._2.extractOpt[String] match {
                      // matches JString(v)
                      case Some(value) if value.nonEmpty => JBool(value.toBoolean)
                      // matches JString()
                      case _ => JNull
                    }
                  }
                }
              } catch {
                case e: IllegalArgumentException => throw new InternalServerErrorException(s"${field._2} is not a parsable `Boolean`", e)
              }
            case _ => field
          }
        }).getOrElse(field)
      }).toJson
    })

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", sourceSettings.bootstrapServers)
      .option("subscribe", mappingSource.topicName)
      .option("startingOffsets", mappingSource.startingOffsets)
      .option("inferSchema", value = true)
      .load()
      .select($"value".cast(StringType)) // change the type of message from binary to string
      .withColumn("value", processDataUDF(col("value"))) // replace 'value' column with the processed data
      .select(from_json($"value", schema.get).as("record"))
      .select("record.*")
    df

  }
}
