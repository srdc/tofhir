package io.onfhir.tofhir.model

import org.json4s.JsonAST.{JString, JValue}


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
case class FileSystemSourceSettings(name: String, sourceUri: String, dataFolderPath: String, override val asStream:Boolean=false) extends DataSourceSettings

/**
 *
 * @param name        Human friendly name for the source organization for data source
 * @param sourceUri   Computer friendly canonical url indicating the source of the data (May be used for Resource.meta.source)
 * @param databaseUrl Connection URL of the SQL database
 * @param username    Username for database connection
 * @param password    Password for database connection
 */
case class SqlSourceSettings(name: String, sourceUri: String, databaseUrl: String, username: String, password: String) extends DataSourceSettings

/**
 *
 * @param name             Human friendly name for the source organization for data source
 * @param sourceUri        Computer friendly canonical url indicating the source of the data (May be used for Resource.meta.source)
 * @param bootstrapServers Kafka bootstrap server(s) with port, may be comma seperated list (localhost:9092,localhost:9091)
 */
case class KafkaSourceSettings(name: String, sourceUri: String, bootstrapServers: String) extends DataSourceSettings {
  override val asStream: Boolean = true
}

