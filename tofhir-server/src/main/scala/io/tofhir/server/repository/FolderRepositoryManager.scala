package io.tofhir.server.repository

import io.tofhir.engine.config.ToFhirEngineConfig
import io.tofhir.server.repository.job.JobFolderRepository
import io.tofhir.server.repository.mapping.ProjectMappingFolderRepository
import io.tofhir.server.repository.mappingContext.MappingContextFolderRepository
import io.tofhir.server.repository.project.ProjectFolderRepository
import io.tofhir.server.repository.schema.SchemaFolderRepository
import io.tofhir.server.repository.terminology.TerminologySystemFolderRepository
import io.tofhir.server.repository.terminology.codesystem.CodeSystemFolderRepository
import io.tofhir.server.repository.terminology.conceptmap.ConceptMapFolderRepository

/**
 * Folder/file based implementation of the RepositoryManager where all managed repositories are folder-based.
 *
 * @param toFhirEngineConfig
 */
class FolderRepositoryManager(toFhirEngineConfig: ToFhirEngineConfig) extends IRepositoryManager {

  override val projectRepository: ProjectFolderRepository = new ProjectFolderRepository(toFhirEngineConfig)
  override val mappingRepository: ProjectMappingFolderRepository = new ProjectMappingFolderRepository(toFhirEngineConfig.mappingRepositoryFolderPath, projectRepository)
  override val schemaRepository: SchemaFolderRepository = new SchemaFolderRepository(toFhirEngineConfig.schemaRepositoryFolderPath, projectRepository)
  override val mappingJobRepository: JobFolderRepository = new JobFolderRepository(toFhirEngineConfig.jobRepositoryFolderPath, projectRepository)
  override val mappingContextRepository: MappingContextFolderRepository = new MappingContextFolderRepository(toFhirEngineConfig.mappingContextRepositoryFolderPath, projectRepository)

  override val terminologySystemRepository: TerminologySystemFolderRepository = new TerminologySystemFolderRepository(toFhirEngineConfig.terminologySystemFolderPath)
  override val conceptMapRepository: ConceptMapFolderRepository = new ConceptMapFolderRepository(toFhirEngineConfig.terminologySystemFolderPath)
  override val codeSystemRepository: CodeSystemFolderRepository = new CodeSystemFolderRepository(toFhirEngineConfig.terminologySystemFolderPath)

  private val folderDBInitializer = new FolderDBInitializer(
    projectRepository,
    schemaRepository,
    mappingRepository,
    mappingJobRepository,
    mappingContextRepository
  )

  /**
   * Initializes the Repository Manager's internal database (the projects.json file) after initialization of
   * each individual repository.
   */
  override def init(): Unit = {
    folderDBInitializer.init()
  }

  /**
   * Deletes the internal repository database (the projects.json file) for a fresh start (usually after cache invalidate operations)
   */
  override def clear(): Unit = {
    folderDBInitializer.removeProjectsJsonFile()
  }

}
