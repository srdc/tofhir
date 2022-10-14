package io.tofhir.server.endpoint

import akka.http.scaladsl.server.Directives._
import io.tofhir.server.common.interceptor.ICORSHandler
import io.tofhir.server.common.config.WebServerConfig

/**
 * Encapsulates all services and directives
 * Main Endpoint for toFHIR server
 */
class ToFhirEndpoint(webServerConfig: WebServerConfig) extends ICORSHandler {
  val toFHIRRoute =
  // logging requests and responses; enabled when necessary for debugging
  //logRequestResponse("REST API", Logging.InfoLevel) {
    pathPrefix(webServerConfig.baseUri) {
      corsHandler {
        null
      }
    }
}
