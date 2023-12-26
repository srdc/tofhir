package io.tofhir.log.server.config

import com.typesafe.config.Config

/**
 * ToFhir log-server configurations
 */
object ToFhirLogServerConfig {

  /**
   * Get config file
   */
  import io.tofhir.engine.Execution.actorSystem
  protected lazy val config: Config = actorSystem.settings.config

  /**
   * Config for toFhir
   */
  lazy val toFhirConfig: Config = config.getConfig("tofhir")

  /** Path of the file that contains results of mapping executions */
  lazy val mappingLogsFilePath: String = toFhirConfig.getString("logs-path")

}
