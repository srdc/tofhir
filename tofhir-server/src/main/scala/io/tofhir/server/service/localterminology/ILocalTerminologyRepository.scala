package io.tofhir.server.service.localterminology

import io.tofhir.server.model.LocalTerminology

import scala.concurrent.Future

/**
 * Interface to save and load LocalTerminology
 * so that the client applications can manage the LocalTerminology through CRUD operations
 */
trait ILocalTerminologyRepository {

  /**
   * Retrieve the metadata of all LocalTerminologyServices
   * @return Seq[LocalTerminology]
   */
  def getAllLocalTerminologyMetadata: Future[Seq[LocalTerminology]]

  /**
   * Create a new LocalTerminologyService
   * @return LocalTerminologyService
   */
  def createTerminologyService(terminology: LocalTerminology): Future[LocalTerminology]

  /**
   * Retrieve a LocalTerminologyService
   * @param id id of the LocalTerminologyService
   * @return LocalTerminologyService if found
   */
  def retrieveTerminology(id: String): Future[LocalTerminology]

  /**
   * Update a LocalTerminologyService
   * @param id id of the LocalTerminologyService
   * @param terminology LocalTerminologyService to update
   * @return updated LocalTerminologyService
   */
  def updateTerminologyService(id: String, terminology: LocalTerminology): Future[LocalTerminology]

  /**
   * Remove a LocalTerminologyService
   * @param id id of the LocalTerminologyService
   * @return
   */
  def removeTerminology(id: String): Future[Unit]
}


