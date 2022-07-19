package io.onfhir.tofhir.model

/**
 * FHIR Mapping task instance
 *
 * @param mappingRef Canonical URL of the FhirMapping definition to execute
 */
case class FhirMappingTask(mappingRef: String, sourceContext: Map[String, FhirMappingSourceContext])

/**
 * Interface for source contexts
 */
trait FhirMappingSourceContext extends Serializable

/**
 * Context/configuration for one of the source of the mapping that will read the source data from file system
 *
 * @param path       File path to the source file
 * @param sourceType Source format for the file See[SourceFileFormats]
 */
case class FileSystemSource(path: String) extends FhirMappingSourceContext {
  def sourceType:String = path.split('.').last
}
/**
 * Context/configuration for one of the source of the mapping that will read the source data from an SQL database
 * Any of tableName and query must be defined. Not both, not neither
 *
 * @param tableName Name of the table
 * @param query     Query to execute in the database
 */
case class SqlSource(tableName: Option[String] = None, query: Option[String] = None) extends FhirMappingSourceContext

/**
 * Context/configuration for one of the source of the mapping that will read the source data from a kafka as stream
 *
 * @param topicName       The topic(s) to subscribe, may be comma seperated string list (topic1,topic2)
 * @param groupId         The Kafka group id to use in Kafka consumer while reading from Kafka
 * @param startingOffsets The start point when a query is started
 */
case class KafkaSource(topicName: String, groupId: String, startingOffsets: String) extends FhirMappingSourceContext

/**
 * List of source file formats supported by tofhir
 */
object SourceFileFormats {
  final val CSV = "csv"
  final val PARQUET = "parquet"
  final val JSON = "json"
  final val AVRO = "avro"
}

