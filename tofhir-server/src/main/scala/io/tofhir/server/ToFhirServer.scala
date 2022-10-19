package io.tofhir.server

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import io.tofhir.server.common.config.WebServerConfig
import io.tofhir.server.endpoint.ToFhirServerEndpoint

object ToFhirServer {
  def start(): Unit = {
    implicit val actorSystem = ActorSystem(Behaviors.empty, "my-system")

    val toFhirConfig = actorSystem.settings.config.getConfig("tofhir")
    val webServerConfig = new WebServerConfig(toFhirConfig.getConfig("webserver"))
    val endpoint = new ToFhirServerEndpoint(webServerConfig)

    ToFhirHttpServer.start(endpoint.toFHIRRoute, webServerConfig)
  }
}
