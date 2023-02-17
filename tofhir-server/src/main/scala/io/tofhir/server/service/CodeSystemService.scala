package io.tofhir.server.service

import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.server.model.TerminologyCodeSystem
import io.tofhir.server.service.terminology.codesystem.{CodeSystemRepository, ICodeSystemRepository}

import scala.concurrent.Future

class CodeSystemService(localTerminologyRepositoryRoot: String) extends LazyLogging {

  private val codeSystemRepository: ICodeSystemRepository = new CodeSystemRepository(localTerminologyRepositoryRoot)

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
  def getCodeSystem(terminologyId: String, codeSystemId: String): Future[Option[TerminologyCodeSystem]] = {
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
   * Upload a CodeSystem csv file for a terminology
   * @param terminologyId id of the terminology
   * @param codeSystemId id of the code system
   * @param byteSource Source of the csv file
   * @return
   */
  def uploadCodeSystemFile(terminologyId: String, codeSystemId: String, byteSource: Source[ByteString, Any]): Future[Unit] = {
    codeSystemRepository.saveCodeSystemContent(terminologyId, codeSystemId, byteSource)
  }

  /**
   * Download a CodeSystem csv file for a terminology
   * @param terminologyId id of the terminology
   * @param codeSystemId id of the code system
   * @return Source of the csv file
   */
  def downloadCodeSystemFile(terminologyId: String, codeSystemId: String): Future[Source[ByteString, Any]] = {
    codeSystemRepository.getCodeSystemContent(terminologyId, codeSystemId)
  }

}
