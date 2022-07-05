package io.onfhir.tofhir.model

import io.onfhir.tofhir.config.MappingErrorHandling.MappingErrorHandling
import org.json4s.JsonAST.{JString, JValue}

import java.util.Properties

/**
 * Interface for data source settings/configurations
 */
trait DataSourceSettings {
  /**
   * Human friendly name for the source organization for data source
   */
  val name: String

  /**
   * Computer friendly canonical url indicating the source of the data (May be used for Resource.meta.source)
   */
  val sourceUri: String

  /**
   * Return the context params that will be supplied to mapping tasks
   *
   * @return
   */
  def getContextParams: Map[String, JValue] = Map.empty

  def toConfigurationContext: (String, ConfigurationContext) =
    "sourceSystem" -> ConfigurationContext(Map("name" -> JString(name), "sourceUri" -> JString(sourceUri)) ++ getContextParams)
}

/**
 *
 * @param name          Human friendly name for the source organization for data source
 * @param sourceUri     Computer friendly canonical url indicating the source of the data (May be used for Resource.meta.source)
 * @param dataFolderPath Path to the folder all source data is located
 */
case class FileSystemSourceSettings(name: String, sourceUri: String, dataFolderPath: String) extends DataSourceSettings

/**
 *
 * @param name          Human friendly name for the source organization for data source
 * @param sourceUri     Computer friendly canonical url indicating the source of the data (May be used for Resource.meta.source)
 * @param databaseUrl   Connection URL of the SQL database
 * @param username      Username for database connection
 * @param password      Password for database connection
 */
case class SqlSourceSettings(name: String, sourceUri: String, databaseUrl: String, username: String, password: String) extends DataSourceSettings

/**
 *
 * @param name             Human friendly name for the source organization for data source
 * @param sourceUri        Computer friendly canonical url indicating the source of the data (May be used for Resource.meta.source)
 * @param bootstrapServers Kafka bootstrap server(s) with port, may be comma seperated list (localhost:9092,localhost:9091)
 */
case class StreamingSourceSettings(name: String, sourceUri: String, bootstrapServers: String) extends DataSourceSettings

/**
 * Common interface for sink settings
 */
trait FhirSinkSettings

/**
 * Settings for a FHIR repository to store the mapped resources
 *
 * @param fhirRepoUrl      FHIR endpoint root url
 * @param securitySettings Security settings if target API is secured
 */
case class FhirRepositorySinkSettings(fhirRepoUrl: String, securitySettings: Option[FhirRepositorySecuritySettings] = None,
                                      writeErrorHandling: MappingErrorHandling) extends FhirSinkSettings

/**
 * Security settings for FHIR API access
 *
 * @param clientId                   OpenID Client identifier assigned to toFhir
 * @param clientSecret               OpenID Client secret given to toFhir
 * @param requiredScopes             List of required scores to write the resources
 * @param authzServerTokenEndpoint   Authorization servers token endpoint
 * @param clientAuthenticationMethod Client authentication method
 */
case class FhirRepositorySecuritySettings(clientId: String,
                                          clientSecret: String,
                                          requiredScopes: Seq[String],
                                          authzServerTokenEndpoint: String,
                                          clientAuthenticationMethod: String = "client_secret_basic")


/**
 * Any mapping task instance
 */

/**
 * FHIR Mapping task instance
 *
 * @param mappingRef Canonical URL of the FhirMapping definition to execute
 */
case class FhirMappingTask(mappingRef: String, sourceContext: Map[String, FhirMappingSourceContext])

/**
 * Interface for source contexts
 */
trait FhirMappingSourceContext extends Serializable {
  val settings: DataSourceSettings
}

/**
 * Context/configuration for one of the source of the mapping that will read the source data from file system
 *
 * @param path       File path to the source file
 * @param sourceType Source format for the file See[SourceFileFormats]
 */
case class FileSystemSource(path: String, sourceType: String, override val settings: FileSystemSourceSettings) extends FhirMappingSourceContext

/**
 * Context/configuration for one of the source of the mapping that will read the source data from an SQL database
 * Any of tableName and query must be defined. Not both, not neither
 *
 * @param tableName Name of the table
 * @param query     Query to execute in the database
 * @param settings  Settings for the SQL source
 */
case class SqlSource(tableName: Option[String] = None, query: Option[String] = None, override val settings: SqlSourceSettings) extends FhirMappingSourceContext

/**
 * Context/configuration for one of the source of the mapping that will read the source data from a streaming service
 *
 * @param topicName       The topic(s) to subscribe, may be comma seperated string list (topic1,topic2)
 * @param groupId         The Kafka group id to use in Kafka consumer while reading from Kafka
 * @param startingOffsets The start point when a query is started
 * @param settings        Settings for the stream source
 */
case class StreamingSource(topicName: String, groupId: String, startingOffsets: String, override val settings: StreamingSourceSettings) extends FhirMappingSourceContext

/**
 * List of source file formats supported by tofhir
 */
object SourceFileFormats {
  final val CSV = "csv"
  final val PARQUET = "parquet"
  final val JSON = "json"
  final val AVRO = "avro"
}

case class FhirMappingJob(id: String, schedulingSettings: Option[SchedulingSettings], sourceSettings: DataSourceSettings, sinkSettings: FhirSinkSettings, mappings: Seq[SimpleFhirMappingDefinition], mappingErrorHandling: MappingErrorHandling) {
  def tasks: Seq[FhirMappingTask] = { // Return Seq[FhirMappingTask] from Seq[SimpleFhirMappingDefinition]
    // TODO: This is a dirty solution which assumes that all mapping tasks have the same sourceContext with a single element whose name is "source".
    mappings.map {
      case FileSourceMappingDefinition(filePath, mappingRef) => FhirMappingTask(mappingRef, Map("source" -> FileSystemSource(filePath, SourceFileFormats.CSV, sourceSettings.asInstanceOf[FileSystemSourceSettings])))
      case SqlSourceMappingDefinition(tableName, query, mappingRef) => FhirMappingTask(mappingRef, Map("source" -> SqlSource(tableName, query, sourceSettings.asInstanceOf[SqlSourceSettings])))
      case StreamingSourceMappingDefinition(topicName, groupId, startingOffsets, mappingRef) => FhirMappingTask(mappingRef, Map("source" -> StreamingSource(topicName, groupId, startingOffsets, sourceSettings.asInstanceOf[StreamingSourceSettings])))
    }
  }
}

/**
 * Interface for source mapping definition
 */
trait SimpleFhirMappingDefinition{
  val mappingRef: String
}

/**
 *
 * @param filePath   Path of the file
 * @param mappingRef Mapping url corresponding to mapping repository
 */
case class FileSourceMappingDefinition(filePath: String, mappingRef: String) extends SimpleFhirMappingDefinition

/**
 *
 * @param tableName  Table name to execute select query in the database
 * @param query      Query to execute in the database
 * @param mappingRef Mapping url corresponding to mapping repository
 */

case class SqlSourceMappingDefinition(tableName: Option[String], query: Option[String], mappingRef: String) extends SimpleFhirMappingDefinition

/**
 *
 * @param topicName       The topic(s) to subscribe, may be comma seperated string list (topic1,topic2)
 * @param groupId         The Kafka group id to use in Kafka consumer while reading from Kafka
 * @param startingOffsets The start point when a query is started
 * @param mappingRef      Mapping url corresponding to mapping repository
 */
case class StreamingSourceMappingDefinition(topicName: String, groupId: String, startingOffsets: String, mappingRef: String) extends SimpleFhirMappingDefinition

case class SchedulingSettings(cronExpression: String, initialTime: Option[String])