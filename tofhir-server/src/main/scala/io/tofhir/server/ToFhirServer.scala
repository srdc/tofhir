package io.tofhir.server

import akka.actor.ActorSystem
import io.tofhir.engine.mapping.Execution
import io.tofhir.server.common.config.WebServerConfig
import io.tofhir.server.endpoint.ToFhirEndpoint

object ToFhirServer {
  def start(): Unit = {
    implicit val actorSystem: ActorSystem = Execution.actorSystem

    val toFhirConfig = actorSystem.settings.config.getConfig("tofhir")
    val webServerConfig = new WebServerConfig(toFhirConfig.getConfig("webserver"))
    val endpoint = new ToFhirEndpoint(webServerConfig)

    ToFhirHttpServer.start(endpoint.toFHIRRoute, webServerConfig)
  }
}
