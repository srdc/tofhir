package io.tofhir.server.endpoint

import akka.http.scaladsl.model.{HttpMethod, Uri}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{RejectionHandler, Route}
import io.tofhir.engine.config.ToFhirEngineConfig
import io.tofhir.server.config.WebServerConfig
import io.tofhir.server.fhir.FhirDefinitionsConfig
import io.tofhir.server.interceptor.{ICORSHandler, IErrorHandler}
import io.tofhir.server.model.ToFhirRestCall
import io.tofhir.server.service.db.FolderDBInitializer
import io.tofhir.server.service.project.{IProjectRepository, ProjectFolderRepository}
import io.tofhir.server.service.schema.{ISchemaRepository, SchemaFolderRepository}

import java.util.UUID

/**
 * Encapsulates all services and directives
 * Main Endpoint for toFHIR server
 */
class ToFhirServerEndpoint(toFhirEngineConfig: ToFhirEngineConfig, webServerConfig: WebServerConfig, fhirDefinitionsConfig: FhirDefinitionsConfig) extends ICORSHandler with IErrorHandler {

  val terminologyServiceManagerEndpoint = new TerminologyServiceManagerEndpoint(toFhirEngineConfig)

  val projectRepository: IProjectRepository = new ProjectFolderRepository(toFhirEngineConfig) // creating the repository instance globally as weed a singleton instance
  val schemaRepository: ISchemaRepository = new SchemaFolderRepository(toFhirEngineConfig.schemaRepositoryFolderPath, projectRepository.asInstanceOf[ProjectFolderRepository])
  val projectEndpoint = new ProjectEndpoint(toFhirEngineConfig, schemaRepository, projectRepository)

  val fhirDefinitionsEndpoint = new FhirDefinitionsEndpoint(fhirDefinitionsConfig)

  // initialize database
  new FolderDBInitializer(toFhirEngineConfig, schemaRepository.asInstanceOf[SchemaFolderRepository]).initialize()

  lazy val toFHIRRoute: Route =
    pathPrefix(webServerConfig.baseUri) {
      corsHandler {
        extractMethod { httpMethod: HttpMethod =>
          extractUri { requestUri: Uri =>
            extractRequestEntity { requestEntity =>
              optionalHeaderValueByName("X-Correlation-Id") { correlationId =>
                val restCall = new ToFhirRestCall(method = httpMethod, uri = requestUri, requestId = correlationId.getOrElse(UUID.randomUUID().toString), requestEntity = requestEntity)
                handleRejections(RejectionHandler.default) { // Default rejection handling
                  handleExceptions(exceptionHandler(restCall)) { // Handle exceptions
                    terminologyServiceManagerEndpoint.route(restCall) ~ projectEndpoint.route(restCall) ~ fhirDefinitionsEndpoint.route(restCall)
                  }
                }
              }
            }
          }
        }
      }
    }
}
