package io.tofhir.server.endpoint

import akka.http.scaladsl.model.{HttpMethod, Uri}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{RejectionHandler, Route}
import io.tofhir.engine.config.ToFhirEngineConfig
import io.tofhir.server.config.RedCapServiceConfig
import io.tofhir.server.common.config.WebServerConfig
import io.onfhir.definitions.resource.fhir.FhirDefinitionsConfig
import io.onfhir.definitions.resource.endpoint.FhirDefinitionsEndpoint
import io.tofhir.server.common.interceptor.{ICORSHandler, IErrorHandler}
import io.tofhir.server.common.model.ToFhirRestCall
import io.tofhir.server.repository.job.JobFolderRepository
import io.tofhir.server.repository.mapping.ProjectMappingFolderRepository
import io.tofhir.server.repository.mappingContext.MappingContextFolderRepository
import io.tofhir.server.repository.project.{IProjectRepository, ProjectFolderRepository}
import io.tofhir.server.repository.schema.SchemaFolderRepository
import io.tofhir.server.repository.terminology.{ITerminologySystemRepository, TerminologySystemFolderRepository}
import io.tofhir.server.repository.terminology.codesystem.{CodeSystemFolderRepository, ICodeSystemRepository}
import io.tofhir.server.repository.terminology.conceptmap.{ConceptMapFolderRepository, IConceptMapRepository}
import io.tofhir.server.util.ToFhirRejectionHandler
import io.onfhir.definitions.fhirpath.endpoint.FhirPathFunctionsEndpoint
import io.tofhir.server.repository.{FolderDBInitializer, FolderRepositoryManager, IRepositoryManager}

import java.util.UUID

/**
 * Encapsulates all services and directives
 * Main Endpoint for toFHIR server
 */
class ToFhirServerEndpoint(toFhirEngineConfig: ToFhirEngineConfig, webServerConfig: WebServerConfig, fhirDefinitionsConfig: FhirDefinitionsConfig, redCapServiceConfig: Option[RedCapServiceConfig]) extends ICORSHandler with IErrorHandler {

  private val repositoryManager: IRepositoryManager = new FolderRepositoryManager(toFhirEngineConfig)
  // Initialize repositories by reading the resources available in the file system
  repositoryManager.init()

  private val projectEndpoint = new ProjectEndpoint(
    repositoryManager.schemaRepository,
    repositoryManager.mappingRepository,
    repositoryManager.mappingJobRepository,
    repositoryManager.mappingContextRepository,
    repositoryManager.projectRepository
  )

  val terminologyServiceManagerEndpoint = new TerminologyServiceManagerEndpoint(
    repositoryManager.terminologySystemRepository,
    repositoryManager.conceptMapRepository,
    repositoryManager.codeSystemRepository,
    repositoryManager.mappingJobRepository
  )

  val fhirDefinitionsEndpoint = new FhirDefinitionsEndpoint(fhirDefinitionsConfig)

  val functionLibraryPackages: Seq[String] =
    Seq("io.onfhir.path", "io.tofhir.engine.mapping") ++
      toFhirEngineConfig.functionLibrariesConfig // add external function libraries
        .map(_.libraryPackageNames)
        .getOrElse(Seq.empty)
  val fhirPathFunctionsEndpoint = new FhirPathFunctionsEndpoint(functionLibraryPackages)

  val redcapEndpoint =  redCapServiceConfig.map(config => new RedCapEndpoint(config))
  val fileSystemTreeStructureEndpoint = new FileSystemTreeStructureEndpoint()
  val metadataEndpoint = new MetadataEndpoint(toFhirEngineConfig, webServerConfig, fhirDefinitionsConfig, redCapServiceConfig)

  val reloadEndpoint= new ReloadEndpoint(repositoryManager)

  // Custom rejection handler to send proper messages to user
  val toFhirRejectionHandler: RejectionHandler = ToFhirRejectionHandler.getRejectionHandler()

  lazy val toFHIRRoute: Route =
    pathPrefix(webServerConfig.baseUri) {
      corsHandler {
        withRequestTimeoutResponse(_ => ToFhirRejectionHandler.timeoutResponseHandler()) {
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
                        fhirDefinitionsEndpoint.route(),
                        fhirPathFunctionsEndpoint.route(),
                        fileSystemTreeStructureEndpoint.route(restCall),
                        metadataEndpoint.route(restCall),
                        reloadEndpoint.route(restCall),
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
}
