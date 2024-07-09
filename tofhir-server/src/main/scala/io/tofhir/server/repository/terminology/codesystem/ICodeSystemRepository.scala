package io.tofhir.server.repository.terminology.codesystem

import akka.stream.scaladsl.Source
import akka.util.ByteString
import io.tofhir.server.model.TerminologySystem.TerminologyCodeSystem
import io.tofhir.server.model.csv.CsvHeader

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
  def getCodeSystem(terminologyId: String, codeSystemId: String): Future[TerminologyCodeSystem]

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
  * Update the code system header by its id
  * @param terminologyId terminology id the code system belongs to
  * @param codeSystemId code system id e.g. icd9-to-icd10.csv
  * @param headers new headers to update
  * @return
  */
  def updateCodeSystemHeader(terminologyId: String, codeSystemId: String, headers: Seq[CsvHeader]): Future[Unit]

  /**
   * Retrieve and save the content of a code system csv file within a terminology
   * @param terminologyId id of the terminology
   * @param codeSystemId id of the code system
   * @param content Source of the csv file
   * @return
   */
  def saveCodeSystemContent(terminologyId: String, codeSystemId: String, content: Source[ByteString, Any], pageNumber: Int, pageSize: Int): Future[Long]

  /**
   * Retrieve the content of a code system csv file within a terminology
   * @param terminologyId id of the terminology
   * @param codeSystemId id of the code system
   * @return Source of the csv file
   */
  def getCodeSystemContent(terminologyId: String, codeSystemId: String, pageNumber: Int, pageSize: Int): Future[(Source[ByteString, Any], Long)]

  /**
   * Upload the code system to the repository
   * @param terminologyId id of the terminology
   * @param codeSystemId  id of the code system
   * @param content       Source of the csv file
   * @return
   */
  def uploadCodeSystem(terminologyId: String, codeSystemId: String, content: Source[ByteString, Any]): Future[Unit]

  /**
   * Download the content of a code system csv file within a terminology
   * @param terminologyId id of the terminology
   * @param codeSystemId  id of the code system
   * @return Source of the csv file
   */
  def downloadCodeSystem(terminologyId: String, codeSystemId: String): Future[Source[ByteString, Any]]

}
