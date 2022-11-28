package io.tofhir.server.endpoint

import akka.http.scaladsl.model.{HttpMethod, Uri}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import io.tofhir.server.common.interceptor.ICORSHandler
import io.tofhir.server.common.config.WebServerConfig
import io.tofhir.server.common.model.ToFhirRestCall
import io.tofhir.server.fhir.FhirDefinitionsConfig

import java.util.UUID

/**
 * Encapsulates all services and directives
 * Main Endpoint for toFHIR server
 */
class ToFhirServerEndpoint(webServerConfig: WebServerConfig, fhirDefinitionsConfig: FhirDefinitionsConfig) extends ICORSHandler {

  val fhirDefinitionsEndpoint = new FhirDefinitionsEndpoint(fhirDefinitionsConfig)
  val schemaDefinitionEndpoint = new SchemaDefinitionEndpoint()

  lazy val toFHIRRoute: Route =
    pathPrefix(webServerConfig.baseUri) {
      corsHandler {
        extractMethod { httpMethod: HttpMethod =>
          extractUri { requestUri: Uri =>
            optionalHeaderValueByName("X-Correlation-Id") { correlationId =>
              val restCall = new ToFhirRestCall(method = httpMethod, uri = requestUri, requestId = correlationId.getOrElse(UUID.randomUUID().toString))
              fhirDefinitionsEndpoint.route(restCall) ~ schemaDefinitionEndpoint.route(restCall)
            }
          }
        }
      }
    }
}
