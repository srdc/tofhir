package io.tofhir.server.endpoint

import akka.http.scaladsl.model.{HttpMethod, Uri}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{RejectionHandler, Route}
import io.tofhir.engine.config.ToFhirEngineConfig
import io.tofhir.server.config.WebServerConfig
import io.tofhir.server.fhir.FhirDefinitionsConfig
import io.tofhir.server.interceptor.{ICORSHandler, IErrorHandler}
import io.tofhir.server.model.ToFhirRestCall

import java.util.UUID

/**
 * Encapsulates all services and directives
 * Main Endpoint for toFHIR server
 */
class ToFhirServerEndpoint(toFhirEngineConfig: ToFhirEngineConfig, webServerConfig: WebServerConfig, fhirDefinitionsConfig: FhirDefinitionsConfig) extends ICORSHandler with IErrorHandler {

  val fhirDefinitionsEndpoint = new FhirDefinitionsEndpoint(fhirDefinitionsConfig)
  val schemaDefinitionEndpoint = new SchemaDefinitionEndpoint(toFhirEngineConfig)
  val mappingEndpoint = new MappingEndpoint(toFhirEngineConfig)
  val projectEndpoint = new ProjectEndpoint(toFhirEngineConfig)

  lazy val toFHIRRoute: Route =
    pathPrefix(webServerConfig.baseUri) {
      corsHandler {
        extractMethod { httpMethod: HttpMethod =>
          extractUri { requestUri: Uri =>
            optionalHeaderValueByName("X-Correlation-Id") { correlationId =>
              val restCall = new ToFhirRestCall(method = httpMethod, uri = requestUri, requestId = correlationId.getOrElse(UUID.randomUUID().toString))
              handleRejections(RejectionHandler.default) { // Default rejection handling
                handleExceptions(exceptionHandler(restCall)) { // Handle exceptions
                  fhirDefinitionsEndpoint.route(restCall) ~ schemaDefinitionEndpoint.route(restCall) ~ mappingEndpoint.route(restCall) ~ projectEndpoint.route(restCall)
                }
              }
            }
          }
        }
      }
    }
}
