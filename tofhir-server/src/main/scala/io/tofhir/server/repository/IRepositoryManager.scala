package io.tofhir.server.repository

import io.tofhir.server.repository.job.IJobRepository
import io.tofhir.server.repository.mapping.IMappingRepository
import io.tofhir.server.repository.mappingContext.IMappingContextRepository
import io.tofhir.server.repository.project.IProjectRepository
import io.tofhir.server.repository.schema.ISchemaRepository
import io.tofhir.server.repository.terminology.ITerminologySystemRepository
import io.tofhir.server.repository.terminology.codesystem.ICodeSystemRepository
import io.tofhir.server.repository.terminology.conceptmap.IConceptMapRepository

/**
 * Manage the repositories throughout toFHIR
 */
trait IRepositoryManager {
  val projectRepository: IProjectRepository
  val mappingRepository: IMappingRepository
  val schemaRepository: ISchemaRepository
  val mappingJobRepository: IJobRepository
  val mappingContextRepository: IMappingContextRepository

  val terminologySystemRepository: ITerminologySystemRepository
  val conceptMapRepository: IConceptMapRepository
  val codeSystemRepository: ICodeSystemRepository

  /**
   * Initialize the repository
   */
  def init(): Unit

  /**
   * Clean-up the repository database
   */
  def clear(): Unit
}
