package io.tofhir.server.repository.terminology

import io.tofhir.engine.repository.ICachedRepository
import io.tofhir.server.model.TerminologySystem

import scala.concurrent.Future

/**
 * Interface to save and load TerminologySystem so that the client applications can manage the TerminologySystem
 * through CRUD operations
 */
trait ITerminologySystemRepository extends ICachedRepository{

  /**
   * Retrieve the metadata of all TerminologySystems
   *
   * @return Seq[TerminologySystem]
   */
  def getTerminologySystemsMetadata: Future[Seq[TerminologySystem]]

  /**
   * Create a new TerminologySystem
   *
   * @return TerminologySystem
   */
  def createTerminologySystem(terminology: TerminologySystem): Future[TerminologySystem]

  /**
   * Retrieve a TerminologySystem
   *
   * @param id id of the TerminologySystem
   * @return TerminologySystem if found
   */
  def getTerminologySystem(id: String): Future[Option[TerminologySystem]]

  /**
   * Update a TerminologySystem
   *
   * @param id          id of the TerminologySystem to be updated
   * @param terminology TerminologySystem to update
   * @return updated TerminologySystem
   */
  def updateTerminologySystem(id: String, terminology: TerminologySystem): Future[TerminologySystem]

  /**
   * Remove a TerminologySystem
   *
   * @param id id of the TerminologySystem to be removed
   * @return
   */
  def deleteTerminologySystem(id: String): Future[Unit]
}
