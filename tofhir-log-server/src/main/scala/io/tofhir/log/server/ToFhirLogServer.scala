package io.tofhir.log.server

import io.tofhir.log.server.endpoint.ExecutionEndpoint
import io.tofhir.server.common.config.WebServerConfig

object ToFhirLogServer {
  def start(): Unit = {
    import io.tofhir.engine.Execution.actorSystem

    val webServerConfig = new WebServerConfig(actorSystem.settings.config.getConfig("webserver"))
    val endpoint = new ExecutionEndpoint(webServerConfig)

    ToFhirHttpServer.start(endpoint.toFHIRRoute, webServerConfig)
  }
}
