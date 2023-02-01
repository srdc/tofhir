package io.tofhir.server.service.localterminology.codesystem

import io.onfhir.util.JsonFormatter._
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.server.model._
import io.tofhir.server.service.localterminology.ILocalTerminologyRepository.{TERMINOLOGY_FOLDER, TERMINOLOGY_JSON}
import io.tofhir.server.util.FileOperations
import org.json4s.jackson.Serialization.writePretty

import java.io.{File, FileWriter}
import scala.concurrent.Future

class CodeSystemRepository(localTerminologyRepositoryRoot: String) extends ICodeSystemRepository {

  /**
   * Retrieve the code system within a terminology
   * @return Seq[TerminologyCodeSystem]
   */
  override def getCodeSystems(terminologyId: String): Future[Seq[TerminologyCodeSystem]] = {
    Future {
      val localTerminologyFile = FileOperations.getFileIfExists(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_JSON)
      val localTerminology = FileOperations.readJsonContent(localTerminologyFile, classOf[LocalTerminology])
      localTerminology.find(_.id == terminologyId) match {
        case Some(terminology) =>
          terminology.codeSystems
        case None =>
          throw ResourceNotFound("Local terminology not found.", s"Local terminology with id $terminologyId not found.")
      }
    }
  }

  /**
   * Create a new code system within a terminology
   *
   * @return TerminologyCodeSystem
   */
  override def createCodeSystem(terminologyId: String, codeSystem: TerminologyCodeSystem): Future[TerminologyCodeSystem] = {
    Future {
      val localTerminologyFile = FileOperations.getFileIfExists(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_JSON)
      val localTerminology = FileOperations.readJsonContent(localTerminologyFile, classOf[LocalTerminology])
      // check if id exists in json file
      localTerminology.find(_.id == terminologyId) match {
        case Some(terminology) =>
          val terminologyIdFolder = FileOperations.getFileIfExists(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_FOLDER + File.separator + terminology.folderPath)
          // check if code system id and file name exists in json file
          if (terminology.codeSystems.exists(_.id == codeSystem.id)) {
            throw AlreadyExists("Local terminology code system id already exists.", s"Id ${codeSystem.id} already exists.")
          }
          if (terminology.codeSystems.exists(_.fileName == codeSystem.fileName)) {
            throw AlreadyExists("Local terminology code system file name already exists.", s"File ${codeSystem.fileName} already exists.")
          }
          // check if code system file exists
          val codeSystemFile = new File(terminologyIdFolder, codeSystem.fileName)
          if (codeSystemFile.exists()) {
            throw AlreadyExists("Local terminology code system file already exists.", s"File ${codeSystem.fileName} already exists.")
          }
          // update json file
          val newCodeSystems = terminology.codeSystems :+ codeSystem
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
          // create code system file
          codeSystemFile.createNewFile()
          codeSystem
        case None =>
          throw ResourceNotFound("Local terminology not found.", s"Local terminology with id $terminologyId not found.")
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
      val localTerminologyFile = FileOperations.getFileIfExists(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_JSON)
      val localTerminologies = FileOperations.readJsonContent(localTerminologyFile, classOf[LocalTerminology])
      localTerminologies.find(_.id == terminologyId) match {
        case Some(terminology) =>
          terminology.codeSystems.find(_.id == codeSystemId) match {
            case Some(codeSystem) =>
              codeSystem
            case None =>
              throw ResourceNotFound("Local terminology code system not found.", s"Local terminology code system with id $codeSystemId not found.")
          }
        case None =>
          throw ResourceNotFound("Local terminology not found.", s"Local terminology with id $terminologyId not found.")
      }
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
      val localTerminologyFile = FileOperations.getFileIfExists(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_JSON)
      val localTerminology = FileOperations.readJsonContent(localTerminologyFile, classOf[LocalTerminology])
      localTerminology.find(_.id == terminologyId) match {
        case Some(terminology) =>
          val terminologyIdFolder = FileOperations.getFileIfExists(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_FOLDER + File.separator + terminology.folderPath)
          // check if code system id exists in json file
          if (!terminology.codeSystems.exists(_.id == codeSystemId)) {
            throw ResourceNotFound("Local terminology code system not found.", s"Local terminology code system with id $codeSystemId not found.")
          }
          terminology.codeSystems.find(_.id == codeSystemId) match {
            case Some(foundCodeSystem) =>
              // check if code system file changed and already exists
              val newCodeSystemFile = new File(terminologyIdFolder, codeSystem.fileName)
              if (foundCodeSystem.fileName != codeSystem.fileName && newCodeSystemFile.exists()) {
                throw AlreadyExists("Local terminology code system file already exists.", s"File ${codeSystem.fileName} already exists.")
              }
              // update code system file
              val newCodeSystems = terminology.codeSystems.map(cm =>
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
              // rename code system file
              if (foundCodeSystem.fileName != codeSystem.fileName) {
                val oldCodeSystemFile = new File(terminologyIdFolder, foundCodeSystem.fileName)
                oldCodeSystemFile.renameTo(newCodeSystemFile)
              }
              codeSystem
            case None =>
              throw ResourceNotFound("Local terminology code system not found.", s"Local terminology code system with id $codeSystemId not found.")
          }

        case None =>
          throw ResourceNotFound("Local terminology not found.", s"Local terminology with id $terminologyId not found.")
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
      val localTerminologyFile = FileOperations.getFileIfExists(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_JSON)
      val localTerminology = FileOperations.readJsonContent(localTerminologyFile, classOf[LocalTerminology])
      localTerminology.find(_.id == terminologyId) match {
        case Some(terminology) =>
          val terminologyIdFolder = FileOperations.getFileIfExists(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_FOLDER + File.separator + terminology.folderPath)
          // check if code system id exists in json file
          if (!terminology.codeSystems.exists(_.id == codeSystemId)) {
            throw ResourceNotFound("Local terminology code system not found.", s"Local terminology code system with id $codeSystemId not found.")
          }
          // remove code system from json file
          val newCodeSystems = terminology.codeSystems.filterNot(_.id == codeSystemId)
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
          // remove code system file
          val codeSystemFile = new File(terminologyIdFolder, terminology.codeSystems.find(_.id == codeSystemId).get.fileName)
          codeSystemFile.delete()
        case None =>
          throw ResourceNotFound("Local terminology not found.", s"Local terminology with id $terminologyId not found.")
      }
    }
  }
}


