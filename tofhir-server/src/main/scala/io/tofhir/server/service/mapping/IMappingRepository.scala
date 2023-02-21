package io.tofhir.server.service.mapping

import io.tofhir.engine.model.FhirMapping
import io.tofhir.server.model.FhirMappingMetadata

import scala.concurrent.Future

/**
 * Interface to save and load MappingFiles
 * so that the client applications can manage the mappings through CRUD operations
 */
trait IMappingRepository {

  /**
   * Retrieve the metadata of all MappingFile, filter by subfolder if given
   * @return
   */
  def getAllMappingMetadata(projectId: String): Future[Seq[FhirMappingMetadata]]

  /**
   * Save the mapping to the repository.
   *
   * @param projectId  subfolder to save the mapping in
   * @param mapping mapping to save
   * @return
   */
  def createMapping(projectId: String, mapping: FhirMapping): Future[FhirMapping]

  /**
   * Get the mapping by its id
   * @param projectId project id the mapping belongs to
   * @param id mapping id
   * @return
   */
  def getMapping(projectId: String, id: String): Future[Option[FhirMapping]]

  /**
   * Update the mapping in the repository
   * @param projectId project id the mapping belongs to
   * @param id mapping id
   * @param mapping mapping to save
   * @return
   */
  def putMapping(projectId: String, id: String, mapping: FhirMapping): Future[FhirMapping]

  /**
   * Delete the mapping from the repository
   * @param projectId project id the mapping belongs to
   * @param id mapping id
   * @return
   */
  def deleteMapping(projectId: String, id: String): Future[Unit]
}
