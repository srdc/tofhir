package io.tofhir.server.endpoint

import akka.http.scaladsl.model.{HttpMethod, Uri}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{RejectionHandler, Route}
import io.tofhir.engine.config.ToFhirEngineConfig
import io.tofhir.server.config.{LogServiceConfig, WebServerConfig}
import io.tofhir.server.fhir.FhirDefinitionsConfig
import io.tofhir.server.interceptor.{ICORSHandler, IErrorHandler}
import io.tofhir.server.model.ToFhirRestCall
import io.tofhir.server.service.job.JobFolderRepository
import io.tofhir.server.service.mapping.ProjectMappingFolderRepository
import io.tofhir.server.service.mappingcontext.MappingContextFolderRepository
import io.tofhir.server.service.project.{FolderDBInitializer, ProjectFolderRepository}
import io.tofhir.server.service.schema.SchemaFolderRepository
import io.tofhir.server.service.terminology.TerminologySystemFolderRepository

import java.util.UUID

/**
 * Encapsulates all services and directives
 * Main Endpoint for toFHIR server
 */
class ToFhirServerEndpoint(toFhirEngineConfig: ToFhirEngineConfig, webServerConfig: WebServerConfig, fhirDefinitionsConfig: FhirDefinitionsConfig, logServiceConfig: LogServiceConfig) extends ICORSHandler with IErrorHandler {

  val projectRepository: ProjectFolderRepository = new ProjectFolderRepository(toFhirEngineConfig) // creating the repository instance globally as weed a singleton instance
  val mappingRepository: ProjectMappingFolderRepository = new ProjectMappingFolderRepository(toFhirEngineConfig.mappingRepositoryFolderPath, projectRepository)
  val schemaRepository: SchemaFolderRepository = new SchemaFolderRepository(toFhirEngineConfig.schemaRepositoryFolderPath, projectRepository)
  val mappingJobRepository: JobFolderRepository = new JobFolderRepository(toFhirEngineConfig.jobRepositoryFolderPath, projectRepository)
  val mappingContextRepository: MappingContextFolderRepository = new MappingContextFolderRepository(toFhirEngineConfig.mappingContextRepositoryFolderPath, projectRepository)
  val terminologySystemFolderRepository: TerminologySystemFolderRepository = new TerminologySystemFolderRepository(toFhirEngineConfig.terminologySystemFolderPath)
  // Initialize the projects by reading the resources available in the file system
  new FolderDBInitializer(toFhirEngineConfig, schemaRepository, mappingRepository, mappingJobRepository, projectRepository, mappingContextRepository).init()

  val projectEndpoint = new ProjectEndpoint(schemaRepository, mappingRepository, mappingJobRepository, mappingContextRepository, projectRepository, logServiceConfig.logServiceEndpoint)
  val fhirDefinitionsEndpoint = new FhirDefinitionsEndpoint(fhirDefinitionsConfig)
  val fhirPathFunctionsEndpoint = new FhirPathFunctionsEndpoint()
  val terminologyServiceManagerEndpoint = new TerminologyServiceManagerEndpoint(terminologySystemFolderRepository, mappingJobRepository, toFhirEngineConfig)

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
                    terminologyServiceManagerEndpoint.route(restCall) ~ projectEndpoint.route(restCall) ~ fhirDefinitionsEndpoint.route(restCall) ~ fhirPathFunctionsEndpoint.route(restCall)
                  }
                }
              }
            }
          }
        }
      }
    }
}
