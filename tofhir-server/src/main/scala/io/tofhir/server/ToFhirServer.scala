package io.tofhir.server

import io.tofhir.server.common.config.WebServerConfig
import io.tofhir.server.endpoint.ToFhirServerEndpoint
import io.tofhir.server.fhir.FhirDefinitionsConfig

object ToFhirServer {
  def start(): Unit = {
    import io.tofhir.engine.Execution.actorSystem

    val toFhirConfig = actorSystem.settings.config.getConfig("tofhir")
    val webServerConfig = new WebServerConfig(actorSystem.settings.config.getConfig("webserver"))
    val fhirDefinitionsConfig = new FhirDefinitionsConfig(actorSystem.settings.config.getConfig("fhir"))
    val endpoint = new ToFhirServerEndpoint(webServerConfig, fhirDefinitionsConfig)

    ToFhirHttpServer.start(endpoint.toFHIRRoute, webServerConfig)
  }
}
