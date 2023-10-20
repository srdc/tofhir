package io.tofhir.server

import io.tofhir.engine.config.ToFhirConfig
import io.tofhir.server.config.{LogServiceConfig, WebServerConfig}
import io.tofhir.server.endpoint.ToFhirServerEndpoint
import io.tofhir.server.fhir.FhirDefinitionsConfig

object ToFhirServer {
  def start(): Unit = {
    import io.tofhir.engine.Execution.actorSystem

    val webServerConfig = new WebServerConfig(actorSystem.settings.config.getConfig("webserver"))
    val fhirDefinitionsConfig = new FhirDefinitionsConfig(actorSystem.settings.config.getConfig("fhir"))
    val logServiceConfig = new LogServiceConfig(actorSystem.settings.config.getConfig("log-service"))
    val endpoint = new ToFhirServerEndpoint(ToFhirConfig.engineConfig, webServerConfig, fhirDefinitionsConfig, logServiceConfig)

    ToFhirHttpServer.start(endpoint.toFHIRRoute, webServerConfig)
  }
}
