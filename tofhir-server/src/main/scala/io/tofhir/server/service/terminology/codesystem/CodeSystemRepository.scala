package io.tofhir.server.service.terminology.codesystem

import akka.stream.scaladsl.{FileIO, Source}
import akka.util.ByteString
import io.onfhir.util.JsonFormatter._
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.model.TerminologySystem.TerminologyCodeSystem
import io.tofhir.server.model._
import io.tofhir.server.service.terminology.TerminologySystemFolderRepository.{TERMINOLOGY_SYSTEMS_FOLDER, TERMINOLOGY_SYSTEMS_JSON}
import io.tofhir.server.util.FileOperations
import org.json4s.jackson.Serialization.writePretty

import java.io.FileWriter
import scala.concurrent.Future

class CodeSystemRepository(localTerminologyRepositoryRoot: String) extends ICodeSystemRepository {
  /**
   * Retrieve the code system within a terminology
   *
   * @return Seq[TerminologyCodeSystem]
   */
  override def getCodeSystems(terminologyId: String): Future[Seq[TerminologyCodeSystem]] = {
    Future {
      val localTerminologyFile = FileUtils.getPath(TERMINOLOGY_SYSTEMS_JSON).toFile
      val localTerminology = FileOperations.readJsonContent[TerminologySystem](localTerminologyFile)
      localTerminology.find(_.id == terminologyId).map(_.codeSystems).getOrElse(Seq.empty)
    }
  }

  /**
   * Create a new code system within a terminology
   *
   * @return TerminologyCodeSystem
   */
  override def createCodeSystem(terminologyId: String, codeSystem: TerminologyCodeSystem): Future[TerminologyCodeSystem] = {
    Future {
      val localTerminologyFile = FileUtils.getPath(TERMINOLOGY_SYSTEMS_JSON).toFile
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
          val codeSystemFile = FileUtils.getPath(TERMINOLOGY_SYSTEMS_FOLDER, terminologyId, codeSystem.id).toFile
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
  override def getCodeSystem(terminologyId: String, codeSystemId: String): Future[Option[TerminologyCodeSystem]] = {
    Future {
      val localTerminologyFile = FileUtils.getPath(TERMINOLOGY_SYSTEMS_JSON).toFile
      val localTerminology = FileOperations.readJsonContent[TerminologySystem](localTerminologyFile)
      localTerminology.find(_.id == terminologyId).flatMap(_.codeSystems.find(_.id == codeSystemId))
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
      val localTerminologyFile = FileUtils.getPath(TERMINOLOGY_SYSTEMS_JSON).toFile
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
      val localTerminologyFile = FileUtils.getPath(TERMINOLOGY_SYSTEMS_JSON).toFile
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
          val codeSystemFile = FileUtils.getPath(TERMINOLOGY_SYSTEMS_FOLDER, terminologyId, codeSystemId).toFile
          codeSystemFile.delete()
        case None =>
          throw BadRequest("Local terminology id does not exist.", s"Id $terminologyId does not exist.")
      }
    }
  }
  /**
   * Retrieve and save the content of a code system csv file within a terminology
   *
   * @param terminologyId id of the terminology
   * @param codeSystemId  id of the code system
   * @param content       Source of the csv file
   * @return
   */
  override def saveCodeSystemContent(terminologyId: String, codeSystemId: String, content: Source[ByteString, Any]): Future[Unit] = {
    Future {
      //check if code system id exists in json file
      val localTerminologyFile = FileUtils.getPath(TERMINOLOGY_SYSTEMS_JSON).toFile
      val localTerminology = FileOperations.readJsonContent[TerminologySystem](localTerminologyFile)
      localTerminology.find(_.id == terminologyId) match {
        case Some(t) =>
          if (!t.codeSystems.exists(_.id == codeSystemId)) {
            throw ResourceNotFound("Local terminology code system not found.", s"Local terminology code system with id $codeSystemId not found.")
          }
          // save code system file
          val codeSystemFile = FileUtils.getPath(TERMINOLOGY_SYSTEMS_FOLDER, terminologyId, codeSystemId).toFile
          FileOperations.saveFileContent(codeSystemFile, content)
        case None =>
          throw BadRequest("Local terminology id does not exist.", s"Id $terminologyId does not exist.")
      }
    }
  }

  /**
   * Retrieve the content of a code system csv file within a terminology
   *
   * @param terminologyId id of the terminology
   * @param codeSystemId  id of the code system
   * @return Source of the csv file
   */
  override def getCodeSystemContent(terminologyId: String, codeSystemId: String): Future[Source[ByteString, Any]] = {
    Future {
      // check if code system id exists in json file
      val localTerminologyFile = FileUtils.getPath(TERMINOLOGY_SYSTEMS_JSON).toFile
      val localTerminology = FileOperations.readJsonContent[TerminologySystem](localTerminologyFile)

      localTerminology.find(_.id == terminologyId) match {
        case Some(t) =>
          if (!t.codeSystems.exists(_.id == codeSystemId)) {
            throw ResourceNotFound("Local terminology code system not found.", s"Local terminology code system with id $codeSystemId not found.")
          }
          // get code system file
          val codeSystemFile = FileUtils.getPath(TERMINOLOGY_SYSTEMS_FOLDER, terminologyId, codeSystemId)
          FileIO.fromPath(codeSystemFile)
        case None =>
          throw BadRequest("Local terminology id does not exist.", s"Id $terminologyId does not exist.")
      }
    }
  }
}


