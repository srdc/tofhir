package io.tofhir.server

import io.tofhir.engine.config.ToFhirConfig
import io.tofhir.server.config.RedCapServiceConfig
import io.tofhir.server.common.config.WebServerConfig
import io.tofhir.server.endpoint.ToFhirServerEndpoint
import io.tofhir.server.fhir.FhirDefinitionsConfig

object ToFhirServer {
  def start(): Unit = {
    import io.tofhir.engine.Execution.actorSystem

    val webServerConfig = new WebServerConfig(actorSystem.settings.config.getConfig("webserver"))
    val fhirDefinitionsConfig = new FhirDefinitionsConfig(actorSystem.settings.config.getConfig("fhir"))
    val redCapServiceConfig = new RedCapServiceConfig(actorSystem.settings.config.getConfig("tofhir-redcap"))
    val endpoint = new ToFhirServerEndpoint(ToFhirConfig.engineConfig, webServerConfig, fhirDefinitionsConfig, redCapServiceConfig)

    ToFhirHttpServer.start(endpoint.toFHIRRoute, webServerConfig)
  }
}
