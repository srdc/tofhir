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
 * @param sourceContext     Provide details how to load each source data for the mapping
 * @param mapping           FhirMapping definition to execute
 */
case class FhirMappingTask(mappingRef: String, sourceContext: Map[String, FhirMappingSourceContext], mapping: Option[FhirMapping] = None)


/**
 * Interface for source contexts
 */
trait FhirMappingSourceContext extends Serializable {
  /**
   * SQL Query to preprocess the source data
   */
  val preprocessSql: Option[String] = None
}

/**
 * Context/configuration for one of the sources of the mapping that will read the source data from the file system.
 *
 * For batch jobs, you should either provide a file name in the "path" field with a file extension or provide the file name
 * without an extension in the "path" field and indicate the extension in the "fileFormat" field.
 *
 * Examples:
 *   - With extension in path:
 *     FileSystemSource(path = "data/patients.csv") => Will read "data/patients.csv" file
 *   - Without extension in path (specifying file format separately):
 *     FileSystemSource(path = "data/patients", fileFormat = Some("csv")) => Will read "data/patients.csv" file
 *
 * For streaming jobs, you should provide a folder name in the "path" field and a file format in the "fileFormat" field so that
 * it can read the files with specified file formats in the given folder.
 *
 * Examples:
 *   - Providing folder name and file format:
 *     FileSystemSource(path = "data/streaming/patients", fileFormat = Some("json")) => Will read "json" files in "data/streaming/patients" folder
 * @param path        File path to the source file or folder, e.g., "patients.csv" or "patients".
 * @param fileFormat  Format of the file (csv | json | parquet) if it cannot be inferred from the path, e.g., csv.
 * @param options     Further options for the format (Spark Data source options for the format, e.g., for csv -> https://spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option).
 */
case class FileSystemSource(path: String, fileFormat:Option[String] = None, options:Map[String, String] = Map.empty[String, String], override val preprocessSql: Option[String] = None) extends FhirMappingSourceContext {
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
 */
case class SqlSource(tableName: Option[String] = None, query: Option[String] = None, override val preprocessSql: Option[String] = None) extends FhirMappingSourceContext

/**
 * Context/configuration for one of the source of the mapping that will read the source data from a kafka as stream
 *
 * @param topicName       The topic(s) to subscribe, may be comma seperated string list (topic1,topic2)
 * @param groupId         The Kafka group id to use in Kafka consumer while reading from Kafka
 * @param startingOffsets The start point when a query is started
 */
case class KafkaSource(topicName: String, groupId: String, startingOffsets: String, override val preprocessSql: Option[String] = None) extends FhirMappingSourceContext

/**
 * Represents a mapping source context for FHIR server data.
 *
 * @param resourceType   The type of FHIR resource to query.
 * @param query          An optional query string to filter the FHIR resources.
 * @param preprocessSql  An optional SQL string for preprocessing the data before mapping.
 */
case class FhirServerSource(resourceType: String, query: Option[String] = None, override val preprocessSql: Option[String] = None) extends FhirMappingSourceContext

/**
 * List of source file formats supported by tofhir
 */
object SourceFileFormats {
  final val CSV = "csv"
  final val TSV = "tsv"
  final val PARQUET = "parquet"
  final val JSON = "json"
  final val AVRO = "avro"
}

