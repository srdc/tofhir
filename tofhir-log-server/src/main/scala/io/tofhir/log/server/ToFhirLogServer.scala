package io.tofhir.log.server

import io.tofhir.log.server.config.WebServerConfig
import io.tofhir.log.server.endpoint.ExecutionEndpoint

object ToFhirLogServer {
  def start(): Unit = {
    import io.tofhir.engine.Execution.actorSystem

    val webServerConfig = new WebServerConfig(actorSystem.settings.config.getConfig("webserver"))
    val endpoint = new ExecutionEndpoint(webServerConfig)

    ToFhirLogHttpServer.start(endpoint.toFHIRRoute, webServerConfig)
  }
}
