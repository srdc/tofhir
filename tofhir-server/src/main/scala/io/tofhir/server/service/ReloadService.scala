package io.tofhir.server.service

import io.tofhir.server.repository.job.JobFolderRepository
import io.tofhir.server.repository.mapping.ProjectMappingFolderRepository
import io.tofhir.server.repository.mappingContext.MappingContextFolderRepository
import io.tofhir.server.repository.schema.SchemaFolderRepository
import io.tofhir.server.repository.terminology.TerminologySystemFolderRepository
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.server.service.db.FolderDBInitializer

import scala.concurrent.Future

/**
 * Service for reloading resources from the file system.
 */
class ReloadService(mappingRepository: ProjectMappingFolderRepository,
                    schemaRepository: SchemaFolderRepository,
                    mappingJobRepository: JobFolderRepository,
                    mappingContextRepository: MappingContextFolderRepository,
                    terminologySystemFolderRepository: TerminologySystemFolderRepository,
                    folderDBInitializer: FolderDBInitializer) {

  /**
   * Reload all resources.
   * @return
   */
  def reloadResources(): Future[Unit] = {
    Future{
      mappingRepository.reloadMappingDefinitions()
      schemaRepository.reloadSchemaDefinitions()
      mappingJobRepository.reloadJobDefinitions()
      mappingContextRepository.reloadMappingContextDefinitions()
      terminologySystemFolderRepository.reloadTerminologySystems()
      folderDBInitializer.init()
    }
  }
}
