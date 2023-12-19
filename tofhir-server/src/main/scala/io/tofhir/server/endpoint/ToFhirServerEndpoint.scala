package io.tofhir.server.endpoint

import akka.http.scaladsl.model.{HttpMethod, Uri}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{RejectionHandler, Route}
import io.tofhir.engine.config.ToFhirEngineConfig
import io.tofhir.server.config.LogServiceConfig
import io.tofhir.server.common.config.WebServerConfig
import io.tofhir.server.fhir.FhirDefinitionsConfig
import io.tofhir.server.interceptor.IErrorHandler
import io.tofhir.server.common.interceptor.ICORSHandler
import io.tofhir.server.model.{ToFhirRejectionHandler, ToFhirRestCall}
import io.tofhir.server.service.job.JobFolderRepository
import io.tofhir.server.service.mapping.ProjectMappingFolderRepository
import io.tofhir.server.service.mappingcontext.MappingContextFolderRepository
import io.tofhir.server.service.project.{FolderDBInitializer, ProjectFolderRepository}
import io.tofhir.server.service.schema.SchemaFolderRepository
import io.tofhir.server.service.terminology.codesystem.{CodeSystemRepository, ICodeSystemRepository}
import io.tofhir.server.service.terminology.{ITerminologySystemRepository, TerminologySystemFolderRepository}
import io.tofhir.server.service.terminology.conceptmap.{ConceptMapRepository, IConceptMapRepository}

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
  val terminologySystemFolderRepository: ITerminologySystemRepository = new TerminologySystemFolderRepository(toFhirEngineConfig.terminologySystemFolderPath)
  val conceptMapRepository: IConceptMapRepository = new ConceptMapRepository(toFhirEngineConfig.terminologySystemFolderPath)
  val codeSystemRepository: ICodeSystemRepository = new CodeSystemRepository(toFhirEngineConfig.terminologySystemFolderPath)

  // Initialize the projects by reading the resources available in the file system
  new FolderDBInitializer(toFhirEngineConfig, schemaRepository, mappingRepository, mappingJobRepository, projectRepository, mappingContextRepository).init()

  val projectEndpoint = new ProjectEndpoint(schemaRepository, mappingRepository, mappingJobRepository, mappingContextRepository, projectRepository, logServiceConfig.logServiceEndpoint)
  val fhirDefinitionsEndpoint = new FhirDefinitionsEndpoint(fhirDefinitionsConfig)
  val fhirPathFunctionsEndpoint = new FhirPathFunctionsEndpoint()
  val terminologyServiceManagerEndpoint = new TerminologyServiceManagerEndpoint(terminologySystemFolderRepository, conceptMapRepository, codeSystemRepository, mappingJobRepository)
  val redCapIntegrationModuleEndpoint = new RedCapIntegrationModuleEndpoint()
  // Custom rejection handler to send proper messages to user
  val toFhirRejectionHandler: RejectionHandler = ToFhirRejectionHandler.getRejectionHandler();

  lazy val toFHIRRoute: Route =
    pathPrefix(webServerConfig.baseUri) {
      corsHandler {
        extractMethod { httpMethod: HttpMethod =>
          extractUri { requestUri: Uri =>
            extractRequestEntity { requestEntity =>
              optionalHeaderValueByName("X-Correlation-Id") { correlationId =>
                val restCall = new ToFhirRestCall(method = httpMethod, uri = requestUri, requestId = correlationId.getOrElse(UUID.randomUUID().toString), requestEntity = requestEntity)
                handleRejections(toFhirRejectionHandler) {
                  handleExceptions(exceptionHandler(restCall)) { // Handle exceptions
                    terminologyServiceManagerEndpoint.route(restCall) ~ projectEndpoint.route(restCall) ~ fhirDefinitionsEndpoint.route(restCall) ~ fhirPathFunctionsEndpoint.route(restCall) ~ redCapIntegrationModuleEndpoint.route(restCall)
                  }
                }
              }
            }
          }
        }
      }
    }
}
