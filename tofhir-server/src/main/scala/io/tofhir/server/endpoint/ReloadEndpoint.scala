package io.tofhir.server.endpoint

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.{complete, get, pathEndOrSingleSlash, pathPrefix}
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.server.common.model.ToFhirRestCall
import io.tofhir.server.endpoint.ReloadEndpoint.SEGMENT_RELOAD
import io.tofhir.server.repository.{FolderDBInitializer, IRepositoryManager}
import io.tofhir.server.repository.job.JobFolderRepository
import io.tofhir.server.repository.mapping.ProjectMappingFolderRepository
import io.tofhir.server.repository.mappingContext.MappingContextFolderRepository
import io.tofhir.server.repository.schema.SchemaFolderRepository
import io.tofhir.server.repository.terminology.TerminologySystemFolderRepository
import io.tofhir.server.service.ReloadService

/**
 * Endpoint to reload resources from the file system.
 * */
class ReloadEndpoint(repositoryManager: IRepositoryManager) extends LazyLogging {

  val reloadService: ReloadService = new ReloadService(repositoryManager)

  def route(request: ToFhirRestCall): Route = {
    pathPrefix(SEGMENT_RELOAD) {
      pathEndOrSingleSlash {
        reloadResources
      }
    }
  }

  /**
   * Route to reload all resources
   * @return
   */
  private def reloadResources: Route = {
    get {
      complete {
        reloadService.reloadResources() map { _ =>
          StatusCodes.NoContent
        }
      }
    }
  }
}

object ReloadEndpoint {
  val SEGMENT_RELOAD = "reload"
}
