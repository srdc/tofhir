package io.tofhir.server.service

import com.typesafe.scalalogging.LazyLogging
import io.tofhir.server.model.TerminologyConceptMap
import io.tofhir.server.service.localterminology.conceptmap.{ConceptMapRepository, IConceptMapRepository}

import scala.concurrent.Future

class ConceptMapService(localTerminologyRepositoryRoot: String) extends LazyLogging {

  private val conceptMapRepository: IConceptMapRepository = new ConceptMapRepository(localTerminologyRepositoryRoot)

  /**
   * Get all ConceptMaps for a terminology
   * @param terminologyId id of the terminology
   * @return Seq[TerminologyConceptMap]
   */
  def getConceptMaps(terminologyId: String): Future[Seq[TerminologyConceptMap]] = {
    conceptMapRepository.getConceptMaps(terminologyId)
  }

  /**
   * Create a new ConceptMap for a terminology
   * @param terminologyId id of the terminology
   * @param conceptMap TerminologyConceptMap to create
   * @return created TerminologyConceptMap
   */
  def createConceptMap(terminologyId: String, conceptMap: TerminologyConceptMap): Future[TerminologyConceptMap] = {
    conceptMapRepository.createConceptMap(terminologyId, conceptMap)
  }

  /**
   * Get a ConceptMap for a terminology
   * @param terminologyId id of the terminology
   * @param conceptMapId id of the concept map
   * @return TerminologyConceptMap if found
   */
  def getConceptMap(terminologyId: String, conceptMapId: String): Future[TerminologyConceptMap] = {
    conceptMapRepository.getConceptMap(terminologyId, conceptMapId)
  }

  /**
   * Update a ConceptMap for a terminology
   * @param terminologyId id of the terminology
   * @param conceptMapId id of the concept map
   * @param conceptMap TerminologyConceptMap to update
   * @return updated TerminologyConceptMap
   */
  def updateConceptMap(terminologyId: String, conceptMapId: String, conceptMap: TerminologyConceptMap): Future[TerminologyConceptMap] = {
    conceptMapRepository.updateConceptMap(terminologyId, conceptMapId, conceptMap)
  }

  /**
   * Remove a ConceptMap for a terminology
   * @param terminologyId id of the terminology
   * @param conceptMapId id of the concept map
   * @return
   */
  def removeConceptMap(terminologyId: String, conceptMapId: String): Future[Unit] = {
    conceptMapRepository.removeConceptMap(terminologyId, conceptMapId)
  }

}
