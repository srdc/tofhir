package io.tofhir.server.endpoint

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.{complete, get, pathEndOrSingleSlash, pathPrefix}
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.server.common.model.ToFhirRestCall
import io.tofhir.server.endpoint.ReloadEndpoint.SEGMENT_RELOAD
import io.tofhir.server.repository.job.JobFolderRepository
import io.tofhir.server.repository.mapping.ProjectMappingFolderRepository
import io.tofhir.server.repository.mappingContext.MappingContextFolderRepository
import io.tofhir.server.repository.schema.SchemaFolderRepository
import io.tofhir.server.repository.terminology.TerminologySystemFolderRepository
import io.tofhir.server.service.ReloadService
import io.tofhir.server.service.db.FolderDBInitializer

/**
 * Endpoint to reload resources from the file system.
 * */
class ReloadEndpoint(mappingRepository: ProjectMappingFolderRepository,
                     schemaRepository: SchemaFolderRepository,
                     mappingJobRepository: JobFolderRepository,
                     mappingContextRepository: MappingContextFolderRepository,
                     terminologySystemFolderRepository: TerminologySystemFolderRepository,
                     folderDBInitializer: FolderDBInitializer) extends LazyLogging {

  val reloadService: ReloadService = new ReloadService(
    mappingRepository,
    schemaRepository,
    mappingJobRepository,
    mappingContextRepository,
    terminologySystemFolderRepository,
    folderDBInitializer
  )

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
