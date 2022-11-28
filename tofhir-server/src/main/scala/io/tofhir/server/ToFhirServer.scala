package io.tofhir.server

import io.tofhir.server.common.config.WebServerConfig
import io.tofhir.server.endpoint.ToFhirServerEndpoint

object ToFhirServer {
  def start(): Unit = {
    import io.tofhir.engine.Execution.actorSystem

    val toFhirConfig = actorSystem.settings.config.getConfig("tofhir")
    val webServerConfig = new WebServerConfig(toFhirConfig.getConfig("webserver"))
    val endpoint = new ToFhirServerEndpoint(webServerConfig)

    ToFhirHttpServer.start(endpoint.toFHIRRoute, webServerConfig)
  }
}
