package io.tofhir.server.service.terminology

import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.server.model.TerminologySystem.TerminologyConceptMap
import io.tofhir.server.model.csv.CsvHeader
import io.tofhir.server.repository.terminology.conceptmap.IConceptMapRepository

import scala.concurrent.Future

class ConceptMapService(conceptMapRepository: IConceptMapRepository) extends LazyLogging {

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

  /**
   * Update the concept map header by its id
   * @param terminologyId terminology id the concept map belongs to
   * @param conceptMapId concept map id e.g. icd9-to-icd10.csv
   * @param headers new headers to update
   * @return
   */
  def updateConceptMapHeader(terminologyId: String, conceptMapId: String, headers: Seq[CsvHeader]): Future[Unit] = {
    conceptMapRepository.updateConceptMapHeader(terminologyId, conceptMapId, headers)
  }

  /**
   * Retrieve and save the content of a concept map csv file within a terminology
   * @param terminologyId id of the terminology
   * @param conceptMapId id of the concept map
   * @param byteSource content of the csv file
   * @return
   */
  def saveConceptMapContent(terminologyId: String, conceptMapId: String, byteSource: Source[ByteString, Any], pageNumber: Int, pageSize: Int): Future[Long] = {
    conceptMapRepository.saveConceptMapContent(terminologyId, conceptMapId, byteSource, pageNumber, pageSize)
  }

  /**
   * Retrieve the content of a concept map csv file within a terminology
   * @param terminologyId id of the terminology
   * @param conceptMapId id of the concept map
   * @return content of the csv file
   */
  def getConceptMapContent(terminologyId: String, conceptMapId: String, pageNumber: Int, pageSize: Int): Future[(Source[ByteString, Any], Long)] = {
    conceptMapRepository.getConceptMapContent(terminologyId, conceptMapId, pageNumber, pageSize)
  }

  /**
   * Retrieve and save the content of a concept map csv file within a terminology
   * @param terminologyId id of the terminology
   * @param conceptMapId id of the concept map
   * @param byteSource content of the csv file
   * @return
   */
  def uploadConceptMapFile(terminologyId: String, conceptMapId: String, byteSource: Source[ByteString, Any]): Future[Unit] = {
    conceptMapRepository.uploadConceptMap(terminologyId, conceptMapId, byteSource)
  }

  /**
   * Retrieve the content of a concept map csv file within a terminology
   * @param terminologyId id of the terminology
   * @param conceptMapId id of the concept map
   * @return content of the csv file
   */
  def downloadConceptMapFile(terminologyId: String, conceptMapId: String): Future[Source[ByteString, Any]] = {
    conceptMapRepository.downloadConceptMap(terminologyId, conceptMapId)
  }

}
