package io.tofhir.server.service.mapping

import io.tofhir.engine.mapping.{IFhirMappingCachedRepository, IFhirMappingRepository}
import io.tofhir.engine.model.FhirMapping

import scala.concurrent.Future

/**
 * Interface to save and load mappings so that the client applications can manage the mappings through CRUD operations
 */
trait IMappingRepository extends IFhirMappingCachedRepository {

  /**
   * Retrieve all mappings for the given project
   *
   * @param projectId Identifier of the project for which the mappings to be retrieved
   * @return
   */
  def getAllMappings(projectId: String): Future[Seq[FhirMapping]]

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
