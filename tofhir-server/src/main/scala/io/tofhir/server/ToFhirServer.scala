package io.tofhir.server

import io.tofhir.engine.config.ToFhirConfig
import io.tofhir.server.config.RedCapServiceConfig
import io.tofhir.server.common.config.WebServerConfig
import io.tofhir.server.endpoint.ToFhirServerEndpoint
import io.onfhir.definitions.resource.fhir.FhirDefinitionsConfig

import scala.util.Try

object ToFhirServer {
  def start(): Unit = {
    import io.tofhir.engine.Execution.actorSystem

    val webServerConfig = new WebServerConfig(actorSystem.settings.config.getConfig("webserver"))
    val fhirDefinitionsConfig = new FhirDefinitionsConfig(actorSystem.settings.config.getConfig("fhir"))
    val redCapServiceConfig = Try(new RedCapServiceConfig(actorSystem.settings.config.getConfig("tofhir-redcap"))).toOption
    val endpoint = new ToFhirServerEndpoint(ToFhirConfig.engineConfig, webServerConfig, fhirDefinitionsConfig, redCapServiceConfig)

    ToFhirHttpServer.start(endpoint.toFHIRRoute, webServerConfig)
  }
}
