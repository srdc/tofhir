package io.tofhir.server.service

import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.model.FhirMapping
import io.tofhir.server.service.mapping.{IMappingRepository, MappingFolderRepository}
import io.tofhir.server.service.project.{IProjectRepository, ProjectFolderRepository}

import scala.concurrent.Future

class MappingService(mappingRepository: IMappingRepository, projectRepository: IProjectRepository) extends LazyLogging {

  /**
   * Get all mapping metadata from the mapping repository
   * @param projectId if given, only return the mappings in the given sub-folder
   * @return Seq[MappingFile]
   */
  def getAllMappings(projectId: String): Future[Seq[FhirMapping]] = {
    mappingRepository.getAllMappings(projectId)
  }

  /**
   * Save the mapping to the repository
   * @param projectId sub-folder of the mapping
   * @param mapping mapping to save
   * @return FhirMapping
   */
  def createMapping(projectId: String, mapping: FhirMapping): Future[FhirMapping] = {
    mappingRepository.createMapping(projectId, mapping)
  }

  /**
   * Get the mapping by its id
   * @param projectId project id the mapping belongs to
   * @param id mapping id
   * @return
   */
  def getMapping(projectId: String, id: String): Future[Option[FhirMapping]] = {
    mappingRepository.getMapping(projectId, id)
  }

  /**
   * Delete the mapping from the repository
   * @param projectId project id the mapping belongs to
   * @param id mapping id
   * @param mapping mapping to update
   * @return
   */
  def updateMapping(projectId: String, id: String, mapping: FhirMapping): Future[FhirMapping] = {
    mappingRepository.putMapping(projectId, id, mapping)
  }

  /**
   * Delete the mapping from the repository
   * @param projectId project id the mapping belongs to
   * @param id mapping id
   * @return
   */
  def deleteMapping(projectId: String, id: String): Future[Unit] = {
    mappingRepository.deleteMapping(projectId, id)
  }

}
