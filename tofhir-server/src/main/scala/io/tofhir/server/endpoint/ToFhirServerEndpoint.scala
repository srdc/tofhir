package io.tofhir.server.endpoint

import akka.http.scaladsl.model.{HttpMethod, Uri}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{RejectionHandler, Route}
import io.tofhir.engine.config.ToFhirEngineConfig
import io.tofhir.server.config.RedCapServiceConfig
import io.tofhir.server.common.config.WebServerConfig
import io.tofhir.server.fhir.FhirDefinitionsConfig
import io.tofhir.server.common.interceptor.{ICORSHandler, IErrorHandler}
import io.tofhir.server.common.model.ToFhirRestCall
import io.tofhir.server.repository.job.JobFolderRepository
import io.tofhir.server.repository.mapping.ProjectMappingFolderRepository
import io.tofhir.server.repository.mappingContext.MappingContextFolderRepository
import io.tofhir.server.repository.project.ProjectFolderRepository
import io.tofhir.server.repository.schema.SchemaFolderRepository
import io.tofhir.server.repository.terminology.{ITerminologySystemRepository, TerminologySystemFolderRepository}
import io.tofhir.server.repository.terminology.codesystem.{CodeSystemRepository, ICodeSystemRepository}
import io.tofhir.server.repository.terminology.conceptmap.{ConceptMapRepository, IConceptMapRepository}
import io.tofhir.server.service.db.FolderDBInitializer
import io.tofhir.server.util.ToFhirRejectionHandler

import java.util.UUID

/**
 * Encapsulates all services and directives
 * Main Endpoint for toFHIR server
 */
class ToFhirServerEndpoint(toFhirEngineConfig: ToFhirEngineConfig, webServerConfig: WebServerConfig, fhirDefinitionsConfig: FhirDefinitionsConfig, redCapServiceConfig: Option[RedCapServiceConfig]) extends ICORSHandler with IErrorHandler {

  val projectRepository: ProjectFolderRepository = new ProjectFolderRepository(toFhirEngineConfig) // creating the repository instance globally as weed a singleton instance
  val mappingRepository: ProjectMappingFolderRepository = new ProjectMappingFolderRepository(toFhirEngineConfig.mappingRepositoryFolderPath, projectRepository)
  val schemaRepository: SchemaFolderRepository = new SchemaFolderRepository(toFhirEngineConfig.schemaRepositoryFolderPath, projectRepository)
  val mappingJobRepository: JobFolderRepository = new JobFolderRepository(toFhirEngineConfig.jobRepositoryFolderPath, projectRepository)
  val mappingContextRepository: MappingContextFolderRepository = new MappingContextFolderRepository(toFhirEngineConfig.mappingContextRepositoryFolderPath, projectRepository)
  val terminologySystemFolderRepository: ITerminologySystemRepository = new TerminologySystemFolderRepository(toFhirEngineConfig.terminologySystemFolderPath)
  val conceptMapRepository: IConceptMapRepository = new ConceptMapRepository(toFhirEngineConfig.terminologySystemFolderPath)
  val codeSystemRepository: ICodeSystemRepository = new CodeSystemRepository(toFhirEngineConfig.terminologySystemFolderPath)

  // Initialize the projects by reading the resources available in the file system
  new FolderDBInitializer(schemaRepository, mappingRepository, mappingJobRepository, projectRepository, mappingContextRepository).init()

  val projectEndpoint = new ProjectEndpoint(schemaRepository, mappingRepository, mappingJobRepository, mappingContextRepository, projectRepository)
  val fhirDefinitionsEndpoint = new FhirDefinitionsEndpoint(fhirDefinitionsConfig,schemaRepository, mappingRepository)
  val fhirPathFunctionsEndpoint = new FhirPathFunctionsEndpoint()
  val redcapEndpoint =  redCapServiceConfig.map(config => new RedCapEndpoint(config))
  val fileSystemTreeStructureEndpoint = new FileSystemTreeStructureEndpoint()
  val terminologyServiceManagerEndpoint = new TerminologyServiceManagerEndpoint(terminologySystemFolderRepository, conceptMapRepository, codeSystemRepository, mappingJobRepository)
  val metadataEndpoint = new MetadataEndpoint(toFhirEngineConfig, webServerConfig, fhirDefinitionsConfig, redCapServiceConfig)
  // Custom rejection handler to send proper messages to user
  val toFhirRejectionHandler: RejectionHandler = ToFhirRejectionHandler.getRejectionHandler()

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
                    // RedCap Endpoint is optional, so it will be handled separately
                    val routes = Seq(
                      terminologyServiceManagerEndpoint.route(restCall),
                      projectEndpoint.route(restCall),
                      fhirDefinitionsEndpoint.route(restCall),
                      fhirPathFunctionsEndpoint.route(restCall),
                      fileSystemTreeStructureEndpoint.route(restCall),
                      metadataEndpoint.route(restCall)
                    ) ++ redcapEndpoint.map(_.route(restCall))

                    concat(routes: _*)
                  }
                }
              }
            }
          }
        }
      }
    }
}
