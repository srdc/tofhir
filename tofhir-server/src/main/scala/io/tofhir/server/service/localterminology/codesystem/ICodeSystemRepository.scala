package io.tofhir.server.service.localterminology.codesystem

import akka.stream.scaladsl.Source
import akka.util.ByteString
import io.tofhir.server.model.TerminologyCodeSystem

import scala.concurrent.Future

trait ICodeSystemRepository {

  /**
   * Retrieve the code systems within a terminology
   *
   * @return Seq[TerminologyCodeSystem]
   */
  def getCodeSystems(terminologyId: String): Future[Seq[TerminologyCodeSystem]]

  /**
   * Create a new code system within a terminology
   * @return TerminologyCodeSystem
   */
  def createCodeSystem(terminologyId: String, codeSystem: TerminologyCodeSystem): Future[TerminologyCodeSystem]

  /**
   * Retrieve a code system within a terminology
   * @param terminologyId id of the terminology
   * @param codeSystemId id of the code system
   * @return TerminologyCodeSystem if found
   */
  def getCodeSystem(terminologyId: String, codeSystemId: String): Future[Option[TerminologyCodeSystem]]

  /**
   * Update a code system within a terminology
   * @param terminologyId id of the terminology
   * @param codeSystemId id of the code system
   * @param codeSystem TerminologyCodeSystem to update
   * @return updated TerminologyCodeSystem
   */
  def updateCodeSystem(terminologyId: String, codeSystemId: String, codeSystem: TerminologyCodeSystem): Future[TerminologyCodeSystem]

  /**
   * Remove a code system within a terminology
   * @param terminologyId id of the terminology
   * @param codeSystemId id of the code system
   * @return
   */
  def removeCodeSystem(terminologyId: String, codeSystemId: String): Future[Unit]

  /**
   * Retrieve and save the content of a code system csv file within a terminology
   * @param terminologyId id of the terminology
   * @param codeSystemId id of the code system
   * @param content Source of the csv file
   * @return
   */
  def saveCodeSystemContent(terminologyId: String, codeSystemId: String, content: Source[ByteString, Any]): Future[Unit]

  /**
   * Retrieve the content of a code system csv file within a terminology
   * @param terminologyId id of the terminology
   * @param codeSystemId id of the code system
   * @return Source of the csv file
   */
  def getCodeSystemContent(terminologyId: String, codeSystemId: String): Future[Source[ByteString, Any]]

}
