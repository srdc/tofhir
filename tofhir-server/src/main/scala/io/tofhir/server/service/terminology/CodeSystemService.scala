package io.tofhir.server.service.terminology

import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.server.model.TerminologySystem.TerminologyCodeSystem
import io.tofhir.server.model.csv.CsvHeader
import io.tofhir.server.repository.terminology.codesystem.ICodeSystemRepository

import scala.concurrent.Future

class CodeSystemService(codeSystemRepository: ICodeSystemRepository) extends LazyLogging {

  /**
   * Get all code systems for a terminology
   * @param terminologyId id of the terminology
   * @return Seq[TerminologyCodeSystem]
   */
  def getCodeSystems(terminologyId: String): Future[Seq[TerminologyCodeSystem]] = {
    codeSystemRepository.getCodeSystems(terminologyId)
  }

  /**
   * Create a new CodeSystem for a terminology
   * @param terminologyId id of the terminology
   * @param codeSystem TerminologyCodeSystem to create
   * @return created TerminologyCodeSystem
   */
  def createCodeSystem(terminologyId: String, codeSystem: TerminologyCodeSystem): Future[TerminologyCodeSystem] = {
    codeSystemRepository.createCodeSystem(terminologyId, codeSystem)
  }

  /**
   * Get a CodeSystem for a terminology
   * @param terminologyId id of the terminology
   * @param codeSystemId id of the code system
   * @return TerminologyCodeSystem if found
   */
  def getCodeSystem(terminologyId: String, codeSystemId: String): Future[TerminologyCodeSystem] = {
    codeSystemRepository.getCodeSystem(terminologyId, codeSystemId)
  }

  /**
   * Update a CodeSystem for a terminology
   * @param terminologyId id of the terminology
   * @param codeSystemId id of the code system
   * @param codeSystem TerminologyCodeSystem to update
   * @return updated TerminologyCodeSystem
   */
  def updateCodeSystem(terminologyId: String, codeSystemId: String, codeSystem: TerminologyCodeSystem): Future[TerminologyCodeSystem] = {
    codeSystemRepository.updateCodeSystem(terminologyId, codeSystemId, codeSystem)
  }

  /**
   * Remove a CodeSystem for a terminology
   * @param terminologyId id of the terminology
   * @param codeSystemId id of the code system
   * @return
   */
  def removeCodeSystem(terminologyId: String, codeSystemId: String): Future[Unit] = {
    codeSystemRepository.removeCodeSystem(terminologyId, codeSystemId)
  }

  /**
   * Update the code system header by its id
   * @param terminologyId terminology id the code system belongs to
   * @param codeSystemId code system id e.g. icd9-to-icd10.csv
   * @param headers new headers to update
   * @return
   */
  def updateCodeSystemHeader(terminologyId: String, codeSystemId: String, headers: Seq[CsvHeader]): Future[Unit] = {
    codeSystemRepository.updateCodeSystemHeader(terminologyId, codeSystemId, headers)
  }

  /**
   * Upload a CodeSystem csv file for a terminology
   * @param terminologyId id of the terminology
   * @param codeSystemId id of the code system
   * @param byteSource Source of the csv file
   * @return
   */
  def saveCodeSystemContent(terminologyId: String, codeSystemId: String, byteSource: Source[ByteString, Any], pageNumber: Int, pageSize: Int): Future[Long] = {
    codeSystemRepository.saveCodeSystemContent(terminologyId, codeSystemId, byteSource, pageNumber, pageSize)
  }

  /**
   * Download a CodeSystem csv file for a terminology
   * @param terminologyId id of the terminology
   * @param codeSystemId id of the code system
   * @return Source of the csv file
   */
  def getCodeSystemContent(terminologyId: String, codeSystemId: String, pageNumber: Int, pageSize: Int): Future[(Source[ByteString, Any], Long)] = {
    codeSystemRepository.getCodeSystemContent(terminologyId, codeSystemId, pageNumber, pageSize)
  }

  /**
   * Upload a CodeSystem csv file for a terminology
   * @param terminologyId id of the terminology
   * @param codeSystemId id of the code system
   * @param byteSource Source of the csv file
   * @return
   */
  def uploadCodeSystemFile(terminologyId: String, codeSystemId: String, byteSource: Source[ByteString, Any]): Future[Unit] = {
    codeSystemRepository.uploadCodeSystem(terminologyId, codeSystemId, byteSource)
  }

  /**
   * Download a CodeSystem csv file for a terminology
   * @param terminologyId id of the terminology
   * @param codeSystemId id of the code system
   * @return Source of the csv file
   */
  def downloadCodeSystemFile(terminologyId: String, codeSystemId: String): Future[Source[ByteString, Any]] = {
    codeSystemRepository.downloadCodeSystem(terminologyId, codeSystemId)
  }

}
