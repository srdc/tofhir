package io.tofhir.server.service

import com.typesafe.scalalogging.LazyLogging
import io.tofhir.server.model.LocalTerminology
import io.tofhir.server.service.localterminology.LocalTerminologyFolderRepository

import scala.concurrent.Future

class LocalTerminologyService(localTerminologyRepositoryRoot: String) extends LazyLogging {

  private val localTerminologyRepository: LocalTerminologyFolderRepository = new LocalTerminologyFolderRepository(localTerminologyRepositoryRoot)

  /**
   * Get all LocalTerminology metadata as list
   * @return Seq[LocalTerminology]
   */
  def getAllMetadata: Future[Seq[LocalTerminology]] = {
    localTerminologyRepository.getAllLocalTerminologyMetadata
  }

  /**
   * Create a new LocalTerminologyService
   * @param terminology LocalTerminology to create
   * @return LocalTerminology
   */
  def createTerminologyService(terminology: LocalTerminology): Future[LocalTerminology] = {
    localTerminologyRepository.createTerminologyService(terminology)
  }

  /**
   * Get a LocalTerminologyService by id
   * @param id id of the LocalTerminologyService to retrieve
   * @return LocalTerminologyService
   */
  def getTerminologyServiceById(id: String): Future[LocalTerminology] = {
    localTerminologyRepository.retrieveTerminology(id)
  }

  /**
   * Update a LocalTerminologyService by id
   * @param id id of the LocalTerminologyService to update
   * @param terminology LocalTerminologyService to update
   * @return LocalTerminologyService
   */
  def updateTerminologyServiceById(id: String, terminology: LocalTerminology): Future[LocalTerminology] = {
    localTerminologyRepository.updateTerminologyService(id, terminology)
  }

  /**
   * Remove a LocalTerminologyService by id
   * @param id id of the LocalTerminologyService to remove
   * @return
   */
  def removeTerminologyServiceById(id: String): Future[Unit] = {
    localTerminologyRepository.removeTerminology(id)
  }

}
