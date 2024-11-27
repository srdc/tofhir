package io.tofhir.server.repository.mapping

import io.tofhir.engine.model.FhirMapping
import io.tofhir.engine.repository.mapping.IFhirMappingRepository
import io.tofhir.server.repository.project.IProjectList

import scala.concurrent.Future

/**
 * Interface to save and load mappings so that the client applications can manage the mappings through CRUD operations
 */
trait IMappingRepository extends IFhirMappingRepository with IProjectList[FhirMapping] {

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
   * @param projectId subfolder to save the mapping in
   * @param mapping   mapping to save
   * @return
   */
  def saveMapping(projectId: String, mapping: FhirMapping): Future[FhirMapping]

  /**
   * Get the mapping by its id
   *
   * @param projectId project id the mapping belongs to
   * @param mappingId mapping id
   * @return
   */
  def getMapping(projectId: String, mappingId: String): Future[Option[FhirMapping]]

  /**
   * Update the mapping in the repository
   *
   * @param projectId project id the mapping belongs to
   * @param mappingId mapping id
   * @param mapping   mapping to save
   * @return
   */
  def updateMapping(projectId: String, mappingId: String, mapping: FhirMapping): Future[FhirMapping]

  /**
   * Delete the mapping from the repository
   *
   * @param projectId project id the mapping belongs to
   * @param mappingId mapping id
   * @return
   */
  def deleteMapping(projectId: String, mappingId: String): Future[Unit]

  /**
   * Deletes all mappings associated with a specific project.
   *
   * @param projectId The unique identifier of the project for which mappings should be deleted.
   */
  def deleteAllMappings(projectId: String): Future[Unit]

  /**
   * Retrieves the identifiers of mappings referencing the given schema in their definitions.
   *
   * @param projectId identifier of project whose mappings will be checked
   * @param schemaUrl the url of schema
   * @return the identifiers of mappings referencing the given schema in their definitions
   */
  def getMappingsReferencingSchema(projectId: String, schemaUrl: String): Future[Seq[String]]
}
