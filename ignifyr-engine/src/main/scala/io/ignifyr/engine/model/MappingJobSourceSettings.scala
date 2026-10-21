package io.ignifyr.engine.model

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
  val asStream: Boolean = false

  /**
   * A copy of these settings with the given streaming mode. Settings types without a streaming
   * mode (their `asStream` is fixed) return themselves, so callers can force batch reading
   * (e.g. for mapping tests) without matching on concrete settings types.
   */
  def withAsStream(asStream: Boolean): MappingJobSourceSettings = this

  /**
   * A copy of these settings with the `${ENV_VAR}` placeholders of their fields resolved by `resolve` (see
   * `EnvironmentVariableResolver`). Each settings type decides which of its fields may carry one; the default
   * is none.
   */
  def withResolvedEnvironmentVariables(resolve: String => String): MappingJobSourceSettings = this

  /**
   * Return the context params that will be supplied to mapping tasks
   *
   * @return
   */
  def getContextParams: Map[String, JValue] = Map.empty

  def toConfigurationContext: (String, ConfigurationContext) =
    "sourceSystem" -> ConfigurationContext(
      Map("name" -> JString(name), "sourceUri" -> JString(sourceUri)) ++ getContextParams
    )
}

/**
 *
 * @param name           Human friendly name for the source organization for data source
 * @param sourceUri      Computer friendly canonical url indicating the source of the data (May be used for Resource.meta.source)
 * @param dataFolderPath Path to the folder all source data is located
 * @param asStream       Whether to listen the given folders for new files and run the mapping in stream mode
 */
case class FileSystemSourceSettings(
    name: String,
    sourceUri: String,
    dataFolderPath: String,
    override val asStream: Boolean = false
) extends MappingJobSourceSettings {
  override def withAsStream(asStream: Boolean): MappingJobSourceSettings = copy(asStream = asStream)

  override def withResolvedEnvironmentVariables(resolve: String => String): MappingJobSourceSettings =
    copy(dataFolderPath = resolve(dataFolderPath))
}

/**
 *
 * @param name        Human friendly name for the source organization for data source
 * @param sourceUri   Computer friendly canonical url indicating the source of the data (May be used for Resource.meta.source)
 * @param databaseUrl Connection URL of the SQL database
 * @param username    Username for database connection
 * @param password    Password for database connection
 */
case class SqlSourceSettings(name: String, sourceUri: String, databaseUrl: String, username: String, password: String)
    extends MappingJobSourceSettings
