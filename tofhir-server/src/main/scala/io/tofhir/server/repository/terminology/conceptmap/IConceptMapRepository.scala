package io.tofhir.server.repository.terminology.conceptmap

import akka.stream.scaladsl.Source
import akka.util.ByteString
import io.tofhir.server.model.TerminologySystem.TerminologyConceptMap
import io.tofhir.server.model.csv.CsvHeader

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

  /**
   * Update the concept map header by its id
   * @param terminologyId terminology id the concept map belongs to
   * @param conceptMapId concept map id e.g. icd9-to-icd10.csv
   * @param headers new headers to update
   * @return
   */
  def updateConceptMapHeader(terminologyId: String, conceptMapId: String, headers: Seq[CsvHeader]): Future[Unit]

  /**
   * Retrieve and save the content of a concept map csv file within a terminology
   * @param terminologyId id of the terminology
   * @param conceptMapId  id of the concept map
   * @param content       content of the csv file
   * @return
   */
  def saveConceptMapContent(terminologyId: String, conceptMapId: String, content: Source[ByteString, Any], pageNumber: Int, pageSize: Int): Future[Long]

  /**
   * Retrieve the content of a concept map csv file within a terminology
   * @param terminologyId id of the terminology
   * @param conceptMapId id of the concept map
   * @return content of the csv file
   */
  def getConceptMapContent(terminologyId: String, conceptMapId: String, pageNumber: Int, pageSize: Int): Future[(Source[ByteString, Any], Long)]

  /**
   * Upload the concept map to the repository
   * @param terminologyId id of the terminology
   * @param conceptMapId  id of the concept map
   * @param content       content of the csv file
   * @return
   */
  def uploadConceptMap(terminologyId: String, conceptMapId: String, content: Source[ByteString, Any]): Future[Unit]

  /**
   * Download the content of a concept map csv file within a terminology
   * @param terminologyId id of the terminology
   * @param conceptMapId  id of the concept map
   * @return content of the csv file
   */
  def downloadConceptMap(terminologyId: String, conceptMapId: String): Future[Source[ByteString, Any]]

}
