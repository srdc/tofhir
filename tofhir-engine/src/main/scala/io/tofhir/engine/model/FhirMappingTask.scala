package io.tofhir.engine.model

/**
 * Custom exception class for signaling errors related to missing or invalid file formats.
 *
 * This exception is thrown when a required file format is not provided or is determined
 * to be invalid during processing.
 *
 * @param message A descriptive message providing more information about the exception.
 * @throws RuntimeException This exception extends the RuntimeException class for signaling runtime errors.
 *
 */
class MissingFileFormatException(message: String) extends RuntimeException(message)


/**
 * FHIR Mapping task instance
 * {@link mapping} will be executed if it is provided. Otherwise, the mapping referenced by {@link mappingRef} is
 * retrieved from the repository and executed.
 *
 * @param mappingRef        Canonical URL of the FhirMapping definition to execute
 * @param sourceBinding     A map that provides details on how to load each source data for the mapping.
 *                          It links the source settings of a mapping job to the sources of a mapping.
 * @param mapping           FhirMapping definition to execute
 */
case class FhirMappingTask(mappingRef: String, sourceBinding: Map[String, MappingSourceBinding], mapping: Option[FhirMapping] = None)


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
 * For batch jobs, you should either provide a file name in the "path" field with a file extension or provide the folder name
 * in the "path" field and indicate the extension in the "fileFormat" field. In the latter case, it reads the files with
 * the specified file format in the given folder.
 *
 * Examples for Batch Jobs:
 *   - With extension in path:
 *     FileSystemSource(path = "data/patients.csv") => Will read the "data/patients.csv" file.
 *   - Providing folder name and file format:
 *     FileSystemSource(path = "data/patients", fileFormat = Some("csv")) => Will read all "csv" files in the "data/patients" folder.
 *
 * For streaming jobs, you should provide a folder name in the "path" field and a file format in the "fileFormat" field so that
 * it can read the files with the specified file format in the given folder.
 *
 * Examples for Streaming Jobs:
 *   - Providing folder name and file format:
 *     FileSystemSource(path = "data/streaming/patients", fileFormat = Some("json")) => Will read all "json" files in the "data/streaming/patients" folder.
 * @param path        File path to the source file or folder, e.g., "patients.csv" or "patients".
 * @param fileFormat  Format of the file (csv | json | parquet) if it cannot be inferred from the path, e.g., csv.
 * @param options     Further options for the format (Spark Data source options for the format, e.g., for csv -> https://spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option).
 */
case class FileSystemSource(path: String, fileFormat:Option[String] = None, options:Map[String, String] = Map.empty[String, String], override val preprocessSql: Option[String] = None, override val sourceRef: Option[String] = None) extends MappingSourceBinding {
  /**
   * Infers the file format based on the provided path and file format.
   *
   * This method attempts to determine the file format using the following logic:
   * 1. If `fileFormat` is provided, it is used as the determined format.
   * 2. If `fileFormat` is not provided and the `path` contains an extension (determined by the presence of a period),
   *    the extension of the `path` is used as the format.
   * 3. If neither `fileFormat` is provided nor an extension is found in the `path`, a `MissingFileFormatException` is thrown.
   *
   * @return The inferred file format.
   * @throws MissingFileFormatException If the file format cannot be determined.
   */
  def inferFileFormat: String = {
    var format = fileFormat
    // split the path into segments based on the period ('.')
    val pathSegments = path.split('.')

    // if the file format is empty and path is a file i.e. there are at least two segments in the path,
    // set the file format based on the last segment of the path
    if (format.isEmpty && pathSegments.length > 1) {
      format = Some(pathSegments.last)
    }

    // if the file format is empty, throw an exception
    if (format.isEmpty) {
      throw new MissingFileFormatException("File format is missing for FileSystemSource. Please provide a valid file format.")
    }
    format.get
  }

}
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
 * @param groupId         The Kafka group id to use in Kafka consumer while reading from Kafka
 * @param startingOffsets The start point when a query is started
 * @param options         Further options for Kafka source (Spark Kafka Guide -> https://spark.apache.org/docs/3.4.1/structured-streaming-kafka-integration.html)
 */
case class KafkaSource(topicName: String, groupId: String, startingOffsets: String, options:Map[String, String] = Map.empty[String, String], override val preprocessSql: Option[String] = None, override val sourceRef: Option[String] = None) extends MappingSourceBinding

/**
 * Represents a mapping source binding for FHIR server data.
 *
 * @param resourceType   The type of FHIR resource to query.
 * @param query          An optional query string to filter the FHIR resources.
 * @param preprocessSql  An optional SQL string for preprocessing the data before mapping.
 */
case class FhirServerSource(resourceType: String, query: Option[String] = None, override val preprocessSql: Option[String] = None, override val sourceRef: Option[String] = None) extends MappingSourceBinding

/**
 * List of source file formats supported by tofhir
 */
object SourceFileFormats {
  final val CSV = "csv"
  final val TSV = "tsv"
  final val PARQUET = "parquet"
  final val JSON = "json"
  final val AVRO = "avro"
  final val TXT_NDJSON = "txt-ndjson"
  final val TXT_CSV = "txt-csv"
  final val TXT_TSV = "txt-tsv"
  final val TXT = "txt"
}

