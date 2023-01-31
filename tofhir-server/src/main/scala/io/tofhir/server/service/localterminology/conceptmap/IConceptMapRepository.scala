package io.tofhir.server.service.localterminology.conceptmap

import io.tofhir.server.model.{LocalTerminology, TerminologyConceptMap}

import scala.concurrent.Future

trait IConceptMapRepository {

  /**
   * Retrieve the concept maps within a terminology
   *
   * @return Seq[TerminologyConceptMap]
   */
  def getConceptMaps(terminologyId: String): Future[Seq[TerminologyConceptMap]]

  /**
   * Create a new concept map within a terminology
   * @return TerminologyConceptMap
   */
  def createConceptMap(terminologyId: String, conceptMap: TerminologyConceptMap): Future[TerminologyConceptMap]

  /**
   * Retrieve a concept map within a terminology
   * @param terminologyId id of the terminology
   * @param conceptMapId id of the concept map
   * @return TerminologyConceptMap if found
   */
  def getConceptMap(terminologyId: String, conceptMapId: String): Future[TerminologyConceptMap]

  /**
   * Update a concept map within a terminology
   * @param terminologyId id of the terminology
   * @param conceptMapId id of the concept map
   * @param conceptMap TerminologyConceptMap to update
   * @return updated TerminologyConceptMap
   */
  def updateConceptMap(terminologyId: String, conceptMapId: String, conceptMap: TerminologyConceptMap): Future[TerminologyConceptMap]

  /**
   * Remove a concept map within a terminology
   * @param terminologyId id of the terminology
   * @param conceptMapId id of the concept map
   * @return
   */
  def removeConceptMap(terminologyId: String, conceptMapId: String): Future[Unit]

}
