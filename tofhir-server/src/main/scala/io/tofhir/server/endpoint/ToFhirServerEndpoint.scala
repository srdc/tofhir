package io.tofhir.server.endpoint

import akka.http.scaladsl.model.{HttpMethod, Uri}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{RejectionHandler, Route}
import io.tofhir.server.config.WebServerConfig
import io.tofhir.server.fhir.FhirDefinitionsConfig
import io.tofhir.server.interceptor.{ICORSHandler, IErrorHandler}
import io.tofhir.server.model.ToFhirRestCall

import java.util.UUID

/**
 * Encapsulates all services and directives
 * Main Endpoint for toFHIR server
 */
class ToFhirServerEndpoint(webServerConfig: WebServerConfig, fhirDefinitionsConfig: FhirDefinitionsConfig) extends ICORSHandler with IErrorHandler {

  val fhirDefinitionsEndpoint = new FhirDefinitionsEndpoint(fhirDefinitionsConfig)
  val schemaDefinitionEndpoint = new SchemaDefinitionEndpoint()

  lazy val toFHIRRoute: Route =
    pathPrefix(webServerConfig.baseUri) {
      corsHandler {
        extractMethod { httpMethod: HttpMethod =>
          extractUri { requestUri: Uri =>
            optionalHeaderValueByName("X-Correlation-Id") { correlationId =>
              val restCall = new ToFhirRestCall(method = httpMethod, uri = requestUri, requestId = correlationId.getOrElse(UUID.randomUUID().toString))
              handleRejections(RejectionHandler.default) { // Default rejection handling
                rejectEmptyResponse { // Reject the empty responses
                  handleExceptions(exceptionHandler(restCall)) { // Handle exceptions
                    fhirDefinitionsEndpoint.route(restCall) ~ schemaDefinitionEndpoint.route(restCall)
                  }
                }
              }
            }
          }
        }
      }
    }
}
