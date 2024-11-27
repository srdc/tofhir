package io.tofhir.server.service

import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.server.repository.{FolderDBInitializer, IRepositoryManager}
import io.tofhir.server.repository.job.JobFolderRepository
import io.tofhir.server.repository.mapping.ProjectMappingFolderRepository
import io.tofhir.server.repository.mappingContext.MappingContextFolderRepository
import io.tofhir.server.repository.schema.SchemaFolderRepository
import io.tofhir.server.repository.terminology.TerminologySystemFolderRepository

import scala.concurrent.Future

/**
 * Service for reloading resources from the file system.
 */
class ReloadService(repositoryManager: IRepositoryManager) {

  /**
   * Reload all resources.
   *
   * @return
   */
  def reloadResources(): Future[Unit] = {
    Future {
      repositoryManager.mappingRepository.invalidate()
      repositoryManager.schemaRepository.invalidate()
      repositoryManager.mappingJobRepository.invalidate()
      repositoryManager.mappingContextRepository.invalidate()
      repositoryManager.terminologySystemRepository.invalidate()
      repositoryManager.clear()
      repositoryManager.init()
    }
  }
}
