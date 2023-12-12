package io.tofhir.server.config


import com.typesafe.config.Config


object ToFhirConfig {

  import io.tofhir.engine.Execution.actorSystem
  protected lazy val config: Config = actorSystem.settings.config

  lazy val toFhirConfig: Config = config.getConfig("tofhir")

  /** Path of the file that contains results of mapping executions */
  lazy val mappingLogsFilePath: String = toFhirConfig.getString("logs-path")

}
