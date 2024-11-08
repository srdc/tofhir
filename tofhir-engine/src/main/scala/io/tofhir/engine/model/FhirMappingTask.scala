package io.tofhir.engine.model

/**
 * FHIR Mapping task instance
 * {@link mapping} will be executed if it is provided. Otherwise, the mapping referenced by {@link mappingRef} is
 * retrieved from the repository and executed.
 *
 * @param name              Name of the given FhirMappingTask
 * @param mappingRef        Canonical URL of the FhirMapping definition to execute
 * @param sourceBinding     A map that provides details on how to load each source data for the mapping.
 *                          It links the source settings of a mapping job to the sources of a mapping.
 * @param mapping           FhirMapping definition to execute
 */
case class FhirMappingTask(name: String, mappingRef: String, sourceBinding: Map[String, MappingSourceBinding], mapping: Option[FhirMapping] = None)


/**
 * Interface for defining the mapping source binding in a mapping job.
 *
 * This interface is used to provide a way to link source settings of a mapping job
 * to the corresponding sources in a mapping. Implementations of this interface
 * should specify how to load each source data required by the mapping.
 */
trait MappingSourceBinding extends Serializable {
  /**
   * SQL Query to preprocess the source data
   */
  val preprocessSql: Option[String] = None
  /**
   * Reference to the source specified in the sourceSettings of a mapping job.
   * This optional field can be used to explicitly link a source binding to a particular source setting.
   * If not provided, the source alias from the mapping will be used to determine the source settings,
   * ensuring backward compatibility.
   */
  val sourceRef: Option[String] = None
}

/**
 * Context/configuration for one of the sources of the mapping that will read the source data from the file system.
 *
 * For batch jobs, you should provide the folder name in the "path" field and indicate the content type in the "contentType"
 * field. It reads the files with the specified content type in the given folder.
 *
 * Example for Batch Jobs:
 *   - With extension in path:
 *     FileSystemSource(path = "data/patients.csv") => Will read the "data/patients.csv" file.
 *   - Providing folder name and content type:
 *     FileSystemSource(path = "data/patients", contentType = "csv") => Will read all files in the "data/patients" folder and interpret them as a csv.
 *
 * For streaming jobs, you should provide a folder name in the "path" field and a content type in the "contentType" field so that
 * it can read the files with the specified content type in the given folder.
 *
 * Examples for Streaming Jobs:
 *   - Providing folder name and content type:
 *     FileSystemSource(path = "data/streaming/patients", contentType = "json") => Will read all files in the "data/streaming/patients" folder in json content type.
 * @param path        File path to the source file or folder, e.g., "patients.csv" or "patients".
 * @param contentType Content of the file
 * @param options     Further options for the content type (Spark Data source options for the content type, e.g., for csv -> https://spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option).
 */
case class FileSystemSource(path: String, contentType:String, options:Map[String, String] = Map.empty[String, String], override val preprocessSql: Option[String] = None, override val sourceRef: Option[String] = None) extends MappingSourceBinding {}
/**
 * Context/configuration for one of the source of the mapping that will read the source data from an SQL database
 * Any of tableName and query must be defined. Not both, not neither
 *
 * @param tableName Name of the table
 * @param query     Query to execute in the database
 * @param options   Further options for SQL source (Spark SQL Guide -> https://spark.apache.org/docs/3.4.1/sql-data-sources-jdbc.html ).
 */
case class SqlSource(tableName: Option[String] = None, query: Option[String] = None, options:Map[String, String] = Map.empty[String, String], override val preprocessSql: Option[String] = None, override val sourceRef: Option[String] = None) extends MappingSourceBinding

/**
 * Context/configuration for one of the source of the mapping that will read the source data from a kafka as stream
 *
 * @param topicName       The topic(s) to subscribe, may be comma seperated string list (topic1,topic2)
 * @param options         Further options for Kafka source (Spark Kafka Guide -> https://spark.apache.org/docs/3.5.1/structured-streaming-kafka-integration.html)
 */
case class KafkaSource(topicName: String, options:Map[String, String] = Map.empty[String, String], override val preprocessSql: Option[String] = None, override val sourceRef: Option[String] = None) extends MappingSourceBinding

/**
 * Represents a mapping source binding for FHIR server data.
 *
 * @param resourceType   The type of FHIR resource to query.
 * @param query          An optional query string to filter the FHIR resources.
 * @param preprocessSql  An optional SQL string for preprocessing the data before mapping.
 */
case class FhirServerSource(resourceType: String, query: Option[String] = None, override val preprocessSql: Option[String] = None, override val sourceRef: Option[String] = None) extends MappingSourceBinding

/**
 * List of source content types supported by tofhir
 */
object SourceContentTypes {
  final val CSV = "csv"
  final val TSV = "tsv"
  final val PARQUET = "parquet"
  final val JSON = "json"
  final val AVRO = "avro"
  final val NDJSON = "ndjson"
}

