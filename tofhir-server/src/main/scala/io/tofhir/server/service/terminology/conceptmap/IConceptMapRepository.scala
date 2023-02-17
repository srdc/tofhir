package io.tofhir.server.service.terminology.conceptmap

import akka.stream.scaladsl.Source
import akka.util.ByteString
import io.tofhir.server.model.TerminologyConceptMap

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
  def getConceptMap(terminologyId: String, conceptMapId: String): Future[Option[TerminologyConceptMap]]

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

  /**
   * Retrieve and save the content of a concept map csv file within a terminology
   * @param terminologyId id of the terminology
   * @param conceptMapId  id of the concept map
   * @param content       content of the csv file
   * @return
   */
  def saveConceptMapContent(terminologyId: String, conceptMapId: String, content: Source[ByteString, Any]): Future[Unit]

  /**
   * Retrieve the content of a concept map csv file within a terminology
   * @param terminologyId id of the terminology
   * @param conceptMapId id of the concept map
   * @return content of the csv file
   */
  def getConceptMapContent(terminologyId: String, conceptMapId: String): Future[Source[ByteString, Any]]

}
