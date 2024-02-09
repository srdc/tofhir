package io.tofhir.server.service.terminology.conceptmap

import akka.stream.scaladsl.{FileIO, Source}
import akka.util.ByteString
import io.tofhir.common.model.Json4sSupport.formats
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.common.model.{AlreadyExists, BadRequest, ResourceNotFound}
import io.tofhir.server.model.TerminologySystem.TerminologyConceptMap
import io.tofhir.server.model._
import io.tofhir.server.service.terminology.TerminologySystemFolderRepository.getTerminologySystemsJsonPath
import io.tofhir.server.util.FileOperations
import org.json4s.jackson.Serialization.writePretty

import java.io.FileWriter
import scala.concurrent.Future

class ConceptMapRepository(terminologySystemFolderPath: String) extends IConceptMapRepository {

  /**
   * Retrieve the concept maps within a terminology
   * @return Seq[TerminologyConceptMap]
   */
  override def getConceptMaps(terminologyId: String): Future[Seq[TerminologyConceptMap]] = {
    Future {
      val localTerminologyFile = FileUtils.getPath(getTerminologySystemsJsonPath(terminologySystemFolderPath)).toFile
      val localTerminology = FileOperations.readJsonContent[TerminologySystem](localTerminologyFile)
      localTerminology.find(_.id == terminologyId).map(_.conceptMaps).getOrElse(Seq.empty)
    }
  }

  /**
   * Create a new concept map within a terminology
   *
   * @return TerminologyConceptMap
   */
  override def createConceptMap(terminologyId: String, conceptMap: TerminologyConceptMap): Future[TerminologyConceptMap] = {
    Future {
      val localTerminologyFile = FileUtils.getPath(getTerminologySystemsJsonPath(terminologySystemFolderPath)).toFile
      val localTerminology = FileOperations.readJsonContent[TerminologySystem](localTerminologyFile)
      // check if concept map id exists
      localTerminology.find(_.id == terminologyId) match {
        case Some(t) =>
          if (t.conceptMaps.exists(_.id == conceptMap.id)) {
            throw AlreadyExists("Local terminology concept map id already exists.", s"Id ${conceptMap.id} already exists.")
          }
          // update local terminology file
          val newConceptMaps = t.conceptMaps :+ conceptMap
          val newLocalTerminology = localTerminology.map(t =>
            if (t.id == terminologyId) t.copy(conceptMaps = newConceptMaps)
            else t
          )
          val writer = new FileWriter(localTerminologyFile)
          try {
            writer.write(writePretty(newLocalTerminology))
          } finally {
            writer.close()
          }
          // create concept map file
          val conceptMapFile = FileUtils.getPath(terminologySystemFolderPath, terminologyId, conceptMap.id).toFile
          conceptMapFile.createNewFile()
          conceptMap

        case None =>
          throw BadRequest("Local terminology id does not exist.", s"Id $terminologyId does not exist.")
      }
    }
  }

  /**
   * Retrieve a concept map within a terminology
   *
   * @param terminologyId id of the terminology
   * @param conceptMapId  id of the concept map
   * @return TerminologyConceptMap if found
   */
  override def getConceptMap(terminologyId: String, conceptMapId: String): Future[Option[TerminologyConceptMap]] = {
    Future {
      val localTerminologyFile = FileUtils.getPath(getTerminologySystemsJsonPath(terminologySystemFolderPath)).toFile
      val localTerminology = FileOperations.readJsonContent[TerminologySystem](localTerminologyFile)
      localTerminology.find(_.id == terminologyId).flatMap(_.conceptMaps.find(_.id == conceptMapId))
    }
  }

  /**
   * Update a concept map within a terminology
   *
   * @param terminologyId id of the terminology
   * @param conceptMapId  id of the concept map
   * @param conceptMap    TerminologyConceptMap to update
   * @return updated TerminologyConceptMap
   */
  override def updateConceptMap(terminologyId: String, conceptMapId: String, conceptMap: TerminologyConceptMap): Future[TerminologyConceptMap] = {
    Future {
      // cross check ids
      if (conceptMap.id != conceptMapId) {
        throw BadRequest("Concept map id does not match.", s"Concept map id ${conceptMap.id} does not match $conceptMapId.")
      }
      val localTerminologyFile = FileUtils.getPath(getTerminologySystemsJsonPath(terminologySystemFolderPath)).toFile
      val localTerminology = FileOperations.readJsonContent[TerminologySystem](localTerminologyFile)
      // check if concept map id exists in json file
      localTerminology.find(_.id == terminologyId) match {
        case Some(t) =>
          if (!t.conceptMaps.exists(_.id == conceptMapId)) {
            throw ResourceNotFound("Local terminology concept map not found.", s"Local terminology concept map with id $conceptMapId not found.")
          }
          val newConceptMaps = t.conceptMaps.map(cm =>
            if (cm.id == conceptMapId) conceptMap
            else cm
          )
          val newLocalTerminology = localTerminology.map(t =>
            if (t.id == terminologyId) t.copy(conceptMaps = newConceptMaps)
            else t
          )
          val writer = new FileWriter(localTerminologyFile)
          try {
            writer.write(writePretty(newLocalTerminology))
          } finally {
            writer.close()
          }
          conceptMap
        case None =>
          throw BadRequest("Local terminology id does not exist.", s"Id $terminologyId does not exist.")
      }
    }
  }

  /**
   * Remove a concept map within a terminology
   *
   * @param terminologyId id of the terminology
   * @param conceptMapId  id of the concept map
   * @return
   */
  override def removeConceptMap(terminologyId: String, conceptMapId: String): Future[Unit] = {
    Future {
      val localTerminologyFile = FileUtils.getPath(getTerminologySystemsJsonPath(terminologySystemFolderPath)).toFile
      val localTerminology = FileOperations.readJsonContent[TerminologySystem](localTerminologyFile)

      localTerminology.find(_.id == terminologyId) match {
        case Some(t) =>
          // check if concept map id exists in json file
          if (!t.conceptMaps.exists(_.id == conceptMapId)) {
            throw ResourceNotFound("Local terminology concept map not found.", s"Local terminology concept map with id $conceptMapId not found.")
          }
          // remove concept map from json file
          val newConceptMaps = t.conceptMaps.filterNot(_.id == conceptMapId)
          val newLocalTerminology = localTerminology.map(t =>
            if (t.id == terminologyId) t.copy(conceptMaps = newConceptMaps)
            else t
          )
          val writer = new FileWriter(localTerminologyFile)
          try {
            writer.write(writePretty(newLocalTerminology))
          } finally {
            writer.close()
          }
          // remove concept map file
          val conceptMapFile = FileUtils.getPath(terminologySystemFolderPath, terminologyId, conceptMapId).toFile
          conceptMapFile.delete()
        case None =>
          throw BadRequest("Local terminology id does not exist.", s"Id $terminologyId does not exist.")
      }
    }
  }

  /**
   * Retrieve and save the content of a concept map csv file within a terminology
   *
   * @param terminologyId id of the terminology
   * @param conceptMapId  id of the concept map
   * @param content       content of the csv file
   * @return
   */
  override def saveConceptMapContent(terminologyId: String, conceptMapId: String, content: Source[ByteString, Any]): Future[Unit] = {
    //check if concept map id exists in json file
    val localTerminologyFile = FileUtils.getPath(getTerminologySystemsJsonPath(terminologySystemFolderPath)).toFile
    val localTerminology = FileOperations.readJsonContent[TerminologySystem](localTerminologyFile)
    localTerminology.find(_.id == terminologyId) match {
      case Some(t) =>
        if (!t.conceptMaps.exists(_.id == conceptMapId)) {
          throw ResourceNotFound("Local terminology concept map not found.", s"Local terminology concept map with id $conceptMapId not found.")
        }
        // save concept map file
        val conceptMapFile = FileUtils.getPath(terminologySystemFolderPath, terminologyId, conceptMapId).toFile
        FileOperations.saveFileContent(conceptMapFile, content)
      case None =>
        throw BadRequest("Local terminology id does not exist.", s"Id $terminologyId does not exist.")
    }
  }

  /**
   * Retrieve the content of a concept map csv file within a terminology
   *
   * @param terminologyId id of the terminology
   * @param conceptMapId  id of the concept map
   * @return content of the csv file
   */
  override def getConceptMapContent(terminologyId: String, conceptMapId: String): Future[Source[ByteString, Any]] = {
    Future {
      // check if concept map id exists in json file
      val localTerminologyFile = FileUtils.getPath(getTerminologySystemsJsonPath(terminologySystemFolderPath)).toFile
      val localTerminology = FileOperations.readJsonContent[TerminologySystem](localTerminologyFile)

      localTerminology.find(_.id == terminologyId) match {
        case Some(t) =>
          if (!t.conceptMaps.exists(_.id == conceptMapId)) {
            throw ResourceNotFound("Local terminology concept map not found.", s"Local terminology concept map with id $conceptMapId not found.")
          }
          // get concept map file
          val conceptMapFile = FileUtils.getPath(terminologySystemFolderPath, terminologyId, conceptMapId)
          FileIO.fromPath(conceptMapFile)
        case None =>
          throw BadRequest("Local terminology id does not exist.", s"Id $terminologyId does not exist.")
      }
    }
  }
}


