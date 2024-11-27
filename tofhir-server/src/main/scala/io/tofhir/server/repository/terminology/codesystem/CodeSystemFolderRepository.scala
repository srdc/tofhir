package io.tofhir.server.repository.terminology.codesystem

import akka.stream.scaladsl.{FileIO, Source}
import akka.util.ByteString
import io.onfhir.definitions.common.model.Json4sSupport.formats
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.common.model.{AlreadyExists, BadRequest, ResourceNotFound}
import io.tofhir.server.model.TerminologySystem.TerminologyCodeSystem
import io.tofhir.server.model._
import io.tofhir.server.model.csv.CsvHeader
import io.tofhir.server.repository.terminology.TerminologySystemFolderRepository.getTerminologySystemsJsonPath
import io.tofhir.server.util.{CsvUtil, FileOperations}
import org.json4s.jackson.Serialization.writePretty

import java.io.FileWriter
import scala.concurrent.Future

class CodeSystemFolderRepository(terminologySystemFolderPath: String) extends ICodeSystemRepository {
  /**
   * Retrieve the code system within a terminology
   *
   * @return Seq[TerminologyCodeSystem]
   */
  override def getCodeSystems(terminologyId: String): Future[Seq[TerminologyCodeSystem]] = {
    Future {
      findLocalTerminologyById(terminologyId).codeSystems
    }
  }

  /**
   * Create a new code system within a terminology
   *
   * @return TerminologyCodeSystem
   */
  override def createCodeSystem(terminologyId: String, codeSystem: TerminologyCodeSystem): Future[TerminologyCodeSystem] = {
    Future {
      val localTerminologyFile = FileUtils.getPath(getTerminologySystemsJsonPath(terminologySystemFolderPath)).toFile
      val localTerminology = FileOperations.readJsonContent[TerminologySystem](localTerminologyFile)
      // check if code system id exists
      localTerminology.find(_.id == terminologyId) match {
        case Some(t) =>
          if (t.codeSystems.exists(_.id == codeSystem.id)) {
            throw AlreadyExists("Local terminology code system id already exists.", s"Id ${codeSystem.id} already exists.")
          }
          // update local terminology file
          val newConceptMaps = t.codeSystems :+ codeSystem
          val newLocalTerminology = localTerminology.map(t =>
            if (t.id == terminologyId) t.copy(codeSystems = newConceptMaps)
            else t
          )
          val writer = new FileWriter(localTerminologyFile)
          try {
            writer.write(writePretty(newLocalTerminology))
          } finally {
            writer.close()
          }
          // create code system file
          val codeSystemFile = FileUtils.getPath(terminologySystemFolderPath, terminologyId, codeSystem.id).toFile
          codeSystemFile.createNewFile()
          codeSystem

        case None =>
          throw BadRequest("Local terminology id does not exist.", s"Id $terminologyId does not exist.")
      }
    }
  }

  /**
   * Retrieve a code system within a terminology
   *
   * @param terminologyId id of the terminology
   * @param codeSystemId  id of the code system
   * @return TerminologyCodeSystem if found
   */
  override def getCodeSystem(terminologyId: String, codeSystemId: String): Future[TerminologyCodeSystem] = {
    Future {
      findCodeSystemById(terminologyId, codeSystemId)
    }
  }

  /**
   * Update a code system within a terminology
   *
   * @param terminologyId id of the terminology
   * @param codeSystemId  id of the code system
   * @param codeSystem    TerminologyCodeSystem to update
   * @return updated TerminologyCodeSystem
   */
  override def updateCodeSystem(terminologyId: String, codeSystemId: String, codeSystem: TerminologyCodeSystem): Future[TerminologyCodeSystem] = {
    Future {
      // cross check ids
      if (codeSystem.id != codeSystemId) {
        throw BadRequest("Code system id does not match.", s"Code system id ${codeSystem.id} does not match $codeSystemId.")
      }
      val localTerminologyFile = FileUtils.getPath(getTerminologySystemsJsonPath(terminologySystemFolderPath)).toFile
      val localTerminology = FileOperations.readJsonContent[TerminologySystem](localTerminologyFile)
      // check if code system id exists in json file
      localTerminology.find(_.id == terminologyId) match {
        case Some(t) =>
          if (!t.codeSystems.exists(_.id == codeSystemId)) {
            throw ResourceNotFound("Local terminology code system not found.", s"Local terminology code system with id $codeSystemId not found.")
          }
          val newCodeSystems = t.codeSystems.map(cm =>
            if (cm.id == codeSystemId) codeSystem
            else cm
          )
          val newLocalTerminology = localTerminology.map(t =>
            if (t.id == terminologyId) t.copy(codeSystems = newCodeSystems)
            else t
          )
          val writer = new FileWriter(localTerminologyFile)
          try {
            writer.write(writePretty(newLocalTerminology))
          } finally {
            writer.close()
          }
          codeSystem
        case None =>
          throw BadRequest("Local terminology id does not exist.", s"Id $terminologyId does not exist.")
      }
    }
  }

  /**
   * Remove a code system within a terminology
   *
   * @param terminologyId id of the terminology
   * @param codeSystemId  id of the code system
   * @return
   */
  override def removeCodeSystem(terminologyId: String, codeSystemId: String): Future[Unit] = {
    Future {
      val localTerminologyFile = FileUtils.getPath(getTerminologySystemsJsonPath(terminologySystemFolderPath)).toFile
      val localTerminology = FileOperations.readJsonContent[TerminologySystem](localTerminologyFile)
      localTerminology.find(_.id == terminologyId) match {
        case Some(t) =>
          // check if code system id exists in json file
          if (!t.codeSystems.exists(_.id == codeSystemId)) {
            throw ResourceNotFound("Local terminology code system not found.", s"Local terminology code system with id $codeSystemId not found.")
          }
          // remove code system from json file
          val newCodeSystems = t.codeSystems.filterNot(_.id == codeSystemId)
          val newLocalTerminology = localTerminology.map(t =>
            if (t.id == terminologyId) t.copy(codeSystems = newCodeSystems)
            else t
          )
          val writer = new FileWriter(localTerminologyFile)
          try {
            writer.write(writePretty(newLocalTerminology))
          } finally {
            writer.close()
          }
          // remove concept map file
          val codeSystemFile = FileUtils.getPath(terminologySystemFolderPath, terminologyId, codeSystemId).toFile
          codeSystemFile.delete()
        case None =>
          throw BadRequest("Local terminology id does not exist.", s"Id $terminologyId does not exist.")
      }
    }
  }

  /**
   * Update the code system header by its id
   * @param terminologyId terminology id the code system belongs to
   * @param codeSystemId code system id
   * @param headers new headers to update
   * @return
   */
  def updateCodeSystemHeader(terminologyId: String, codeSystemId: String, headers: Seq[CsvHeader]): Future[Unit] = {
    val codeSystem = findCodeSystemById(terminologyId, codeSystemId)
    // get file and update headers
    val codeSystemFile = FileUtils.getPath(terminologySystemFolderPath, terminologyId, codeSystem.id).toFile
    CsvUtil.writeCsvHeaders(codeSystemFile, headers)
  }

  /**
   * Retrieve and save the content of a code system csv file within a terminology
   *
   * @param terminologyId id of the terminology
   * @param codeSystemId  id of the code system
   * @param content       Source of the csv file
   * @return
   */
  override def saveCodeSystemContent(terminologyId: String, codeSystemId: String, content: Source[ByteString, Any], pageNumber: Int, pageSize: Int): Future[Long] = {
    val codeSystem = findCodeSystemById(terminologyId, codeSystemId)
    // save code system file
    val codeSystemFile = FileUtils.getPath(terminologySystemFolderPath, terminologyId, codeSystem.id).toFile
    CsvUtil.writeCsvAndReturnRowNumber(codeSystemFile, content, pageNumber, pageSize)
  }

  /**
   * Retrieve the content of a code system csv file within a terminology
   *
   * @param terminologyId id of the terminology
   * @param codeSystemId  id of the code system
   * @return Source of the csv file
   */
  override def getCodeSystemContent(terminologyId: String, codeSystemId: String, pageNumber: Int, pageSize: Int): Future[(Source[ByteString, Any], Long)] = {
    val codeSystem = findCodeSystemById(terminologyId, codeSystemId)
    // get code system file
    val codeSystemFile = FileUtils.getPath(terminologySystemFolderPath, terminologyId, codeSystem.id).toFile
    CsvUtil.getPaginatedCsvContent(codeSystemFile, pageNumber, pageSize)
  }

  /**
   * Upload the code system to the repository
   * @param terminologyId id of the terminology
   * @param codeSystemId  id of the code system
   * @param content       Source of the csv file
   * @return
   */
  override def uploadCodeSystem(terminologyId: String, codeSystemId: String, content: Source[ByteString, Any]): Future[Unit] = {
    val codeSystem = findCodeSystemById(terminologyId, codeSystemId)
    // save code system file
    val codeSystemFile = FileUtils.getPath(terminologySystemFolderPath, terminologyId, codeSystem.id).toFile
    CsvUtil.saveFileContent(codeSystemFile, content)
  }

  /**
   * Download the content of a code system csv file within a terminology
   * @param terminologyId id of the terminology
   * @param codeSystemId  id of the code system
   * @return Source of the csv file
   */
  override def downloadCodeSystem(terminologyId: String, codeSystemId: String): Future[Source[ByteString, Any]] = {
    Future {
      val codeSystem = findCodeSystemById(terminologyId, codeSystemId)
      // get code system file
      val codeSystemFile = FileUtils.getPath(terminologySystemFolderPath, terminologyId, codeSystem.id)
      FileIO.fromPath(codeSystemFile)
    }
  }

  /**
   * Find local terminology by id from json file
   * JSON file contains a list of [[TerminologySystem]]
   * If not found, throw ResourceNotFound
   * @param terminologyId id of the terminology
   * @return
   */
  private def findLocalTerminologyById(terminologyId: String): TerminologySystem = {
    val localTerminologies = getLocalTerminologiesFromFile
    localTerminologies.find(_.id == terminologyId) match {
      case Some(t) => t
      case None => throw ResourceNotFound("Local terminology id does not exist.", s"Id $terminologyId does not exist.")
    }
  }

  /**
   * Find code system by id from a local terminology
   * If either terminology or code system not found, throw ResourceNotFound
   * @param terminologyId id of the terminology
   * @param codeSystemId code system id to find inside the terminology
   * @return
   */
  private def findCodeSystemById(terminologyId: String, codeSystemId: String): TerminologyCodeSystem = {
    val localTerminology = findLocalTerminologyById(terminologyId)
    localTerminology.codeSystems.find(_.id == codeSystemId) match {
      case Some(cs) => cs
      case None => throw ResourceNotFound("Local terminology code system not found.", s"Local terminology code system with id $codeSystemId not found.")
    }
  }

  /**
   * Read terminology systems JSON and return as a list of [[TerminologySystem]]
   * @return
   */
  private def getLocalTerminologiesFromFile: Seq[TerminologySystem] = {
    val localTerminologyFile = FileUtils.getPath(getTerminologySystemsJsonPath(terminologySystemFolderPath)).toFile
    FileOperations.readJsonContent[TerminologySystem](localTerminologyFile)
  }

}


