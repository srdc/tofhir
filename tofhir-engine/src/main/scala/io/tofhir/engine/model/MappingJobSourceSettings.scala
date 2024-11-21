package io.tofhir.engine.model

import io.onfhir.client.model.IFhirRepositorySecuritySettings
import org.json4s.JsonAST.{JString, JValue}


/**
 * Interface for data source settings/configurations of a mapping job
 */
trait MappingJobSourceSettings {
  /**
   * Human friendly name for the source organization for data source
   */
  val name: String

  /**
   * Computer friendly canonical url indicating the source of the data (May be used for Resource.meta.source)
   */
  val sourceUri: String

  /**
   * Whether the data is coming in streaming mode
   */
  val asStream:Boolean = false

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
 * @param name           Human friendly name for the source organization for data source
 * @param sourceUri      Computer friendly canonical url indicating the source of the data (May be used for Resource.meta.source)
 * @param dataFolderPath Path to the folder all source data is located
 * @param asStream       Whether to listen the given folders for new files and run the mapping in stream mode
 */
case class FileSystemSourceSettings(name: String, sourceUri: String, dataFolderPath: String, override val asStream:Boolean=false) extends MappingJobSourceSettings

/**
 *
 * @param name        Human friendly name for the source organization for data source
 * @param sourceUri   Computer friendly canonical url indicating the source of the data (May be used for Resource.meta.source)
 * @param databaseUrl Connection URL of the SQL database
 * @param username    Username for database connection
 * @param password    Password for database connection
 */
case class SqlSourceSettings(name: String, sourceUri: String, databaseUrl: String, username: String, password: String) extends MappingJobSourceSettings

/**
 *
 * @param name             Human friendly name for the source organization for data source
 * @param sourceUri        Computer friendly canonical url indicating the source of the data (May be used for Resource.meta.source)
 * @param bootstrapServers Kafka bootstrap server(s) with port, may be comma seperated list (localhost:9092,localhost:9091)
 * @param asRedCap         Indicate whether it is a RedCap source
 */
case class KafkaSourceSettings(name: String = "", sourceUri: String = "", bootstrapServers: String = "", asRedCap: Boolean = false, override val asStream: Boolean = true) extends MappingJobSourceSettings

/**
 * Settings for configuring a FHIR server data source.
 *
 * @param name      The name of the FHIR server source.
 * @param sourceUri The URI of the FHIR server source.
 * @param serverUrl The URL of the FHIR server.
 * @param securitySettings Security settings if the FHIR Server is secured
 */
case class FhirServerSourceSettings(name: String, sourceUri: String, serverUrl: String, securitySettings: Option[IFhirRepositorySecuritySettings] = None) extends MappingJobSourceSettings
