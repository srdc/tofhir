package io.tofhir.server.service.localterminology.codesystem

import akka.stream.scaladsl.{FileIO, Source}
import akka.util.ByteString
import io.onfhir.util.JsonFormatter._
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.model._
import io.tofhir.server.service.localterminology.LocalTerminologyFolderRepository.{TERMINOLOGY_FOLDER, TERMINOLOGY_JSON}
import io.tofhir.server.util.FileOperations
import org.json4s.jackson.Serialization.writePretty

import java.io.{File, FileWriter}
import scala.collection.mutable
import scala.concurrent.Future

class CodeSystemRepository(localTerminologyRepositoryRoot: String) extends ICodeSystemRepository {

  // terminology id -> code system id -> code system
  private val codeSystems: mutable.Map[String, mutable.Map[String, TerminologyCodeSystem]] = initMap()
  /**
   * Retrieve the code system within a terminology
   *
   * @return Seq[TerminologyCodeSystem]
   */
  override def getCodeSystems(terminologyId: String): Future[Seq[TerminologyCodeSystem]] = {
    Future {
      codeSystems(terminologyId).values.toSeq
    }
  }

  /**
   * Create a new code system within a terminology
   *
   * @return TerminologyCodeSystem
   */
  override def createCodeSystem(terminologyId: String, codeSystem: TerminologyCodeSystem): Future[TerminologyCodeSystem] = {
    Future {
      val localTerminologyFile = FileUtils.getPath(localTerminologyRepositoryRoot, TERMINOLOGY_JSON).toFile
      val localTerminology = FileOperations.readJsonContent[LocalTerminology](localTerminologyFile)
      // check if code system id exists
      if (codeSystems.contains(terminologyId) && codeSystems(terminologyId).contains(codeSystem.id)) {
        throw AlreadyExists("Local terminology code system id already exists.", s"Id ${codeSystem.id} already exists.")
      }
      // update map
      codeSystems.getOrElseUpdate(terminologyId, mutable.Map.empty).put(codeSystem.id, codeSystem)
      // update json file
      val newCodeSystems = codeSystems(terminologyId).values.toSeq :+ codeSystem
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
      val codeSystemFile = FileUtils.getPath(localTerminologyRepositoryRoot, TERMINOLOGY_FOLDER, terminologyId, codeSystem.id).toFile
      codeSystemFile.createNewFile()
      codeSystem
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
      codeSystems(terminologyId).get(codeSystemId)
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
        throw BadRequest("Code system id does not match.", s"Concept map id ${codeSystem.id} does not match $codeSystemId.")
      }
      val localTerminologyFile = FileOperations.getFileIfExists(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_JSON)
      val localTerminology = FileOperations.readJsonContent[LocalTerminology](localTerminologyFile)
      // check if code system id exists in json file
      if (!codeSystems(terminologyId).contains(codeSystemId)) {
        throw ResourceNotFound("Local terminology code system not found.", s"Local terminology code system with id $codeSystemId not found.")
      }
      // update metadata
      val newCodeSystems = codeSystems(terminologyId).values.toSeq.map(cm =>
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
      // update cache
      codeSystems(terminologyId).put(codeSystemId, codeSystem)
      codeSystem
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
      val localTerminology = FileOperations.readJsonContent[LocalTerminology](localTerminologyFile)
      // check if code system id exists in json file
      if (!codeSystems(terminologyId).contains(codeSystemId)) {
        throw ResourceNotFound("Local terminology code system not found.", s"Local terminology code system with id $codeSystemId not found.")
      }
      // remove code system from json file
      val newCodeSystems = codeSystems(terminologyId).values.toSeq.filterNot(_.id == codeSystemId)
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
      val codeSystemFile = FileUtils.getPath(localTerminologyRepositoryRoot, TERMINOLOGY_FOLDER, terminologyId, codeSystems(terminologyId)(codeSystemId).id).toFile
      codeSystemFile.delete()
      // remove from cache
      codeSystems.remove(codeSystemId)
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
      if (!codeSystems(terminologyId).contains(codeSystemId)) {
        throw ResourceNotFound("Local terminology code system not found.", s"Local terminology code system with id $codeSystemId not found.")
      }
      // save code system file
      val codeSystemFile = FileUtils.getPath(localTerminologyRepositoryRoot, TERMINOLOGY_FOLDER, terminologyId, codeSystems(terminologyId)(codeSystemId).id).toFile
      FileOperations.saveFileContent(codeSystemFile, content)
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
      if (!codeSystems(terminologyId).contains(codeSystemId)) {
        throw ResourceNotFound("Local terminology code system not found.", s"Local terminology code system with id $codeSystemId not found.")
      }
      // get code system file
      val codeSystemFile = FileUtils.getPath(localTerminologyRepositoryRoot, TERMINOLOGY_FOLDER, terminologyId, codeSystems(terminologyId)(codeSystemId).id)
      FileIO.fromPath(codeSystemFile)
    }
  }

  /**
   * Initialize the cache
   * @return
   */
  private def initMap(): mutable.Map[String, mutable.Map[String, TerminologyCodeSystem]] = {
    val localTerminologyFile = FileUtils.getPath(localTerminologyRepositoryRoot, TERMINOLOGY_JSON).toFile
    val localTerminologies = FileOperations.readJsonContent[LocalTerminology](localTerminologyFile)
    val map = mutable.Map[String, mutable.Map[String, TerminologyCodeSystem]]()
    localTerminologies.foreach(t => {
      val codeSystems = mutable.Map[String, TerminologyCodeSystem]()
      t.codeSystems.foreach(cd => {
        codeSystems.put(cd.id, cd)
      })
      map.put(t.id, codeSystems)
    })
    map
  }
}


