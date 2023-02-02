package io.tofhir.server.service.localterminology.conceptmap

import akka.stream.scaladsl.{FileIO, Source}
import akka.util.ByteString
import io.onfhir.util.JsonFormatter._
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.server.model.{AlreadyExists, BadRequest, LocalTerminology, ResourceNotFound, TerminologyConceptMap}
import io.tofhir.server.service.localterminology.LocalTerminologyFolderRepository.{TERMINOLOGY_FOLDER, TERMINOLOGY_JSON}
import io.tofhir.server.util.FileOperations
import org.json4s.jackson.Serialization.writePretty

import java.io.{File, FileWriter}
import scala.concurrent.Future

class ConceptMapRepository(localTerminologyRepositoryRoot: String) extends IConceptMapRepository {

  /**
   * Retrieve the concept maps within a terminology
   * @return Seq[TerminologyConceptMap]
   */
  override def getConceptMaps(terminologyId: String): Future[Seq[TerminologyConceptMap]] = {
    Future {
      val localTerminologyFile = FileOperations.getFileIfExists(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_JSON)
      val localTerminology = FileOperations.readJsonContent(localTerminologyFile, classOf[LocalTerminology])
      localTerminology.find(_.id == terminologyId) match {
        case Some(terminology) =>
          terminology.conceptMaps
        case None =>
          throw ResourceNotFound("Local terminology not found.", s"Local terminology with id $terminologyId not found.")
      }
    }
  }

  /**
   * Create a new concept map within a terminology
   *
   * @return TerminologyConceptMap
   */
  override def createConceptMap(terminologyId: String, conceptMap: TerminologyConceptMap): Future[TerminologyConceptMap] = {
    Future {
      val localTerminologyFile = FileOperations.getFileIfExists(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_JSON)
      val localTerminology = FileOperations.readJsonContent(localTerminologyFile, classOf[LocalTerminology])
      // check if id exists in json file
      localTerminology.find(_.id == terminologyId) match {
        case Some(terminology) =>
          val terminologyIdFolder = FileOperations.getFileIfExists(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_FOLDER + File.separator + terminology.name)
          // check if concept map id and file name exists in json file
          if (terminology.conceptMaps.exists(_.id == conceptMap.id)) {
            throw AlreadyExists("Local terminology concept map id already exists.", s"Id ${conceptMap.id} already exists.")
          }
          if (terminology.conceptMaps.exists(_.name == conceptMap.name)) {
            throw AlreadyExists("Local terminology concept map file name already exists.", s"Id ${conceptMap.name} already exists.")
          }
          // check if concept map file exists
          val conceptMapFile = new File(terminologyIdFolder, conceptMap.name)
          if (conceptMapFile.exists()) {
            throw AlreadyExists("Local terminology concept map file already exists.", s"File ${conceptMap.name} already exists.")
          }
          // update json file
          val newConceptMaps = terminology.conceptMaps :+ conceptMap
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
          conceptMapFile.createNewFile()
          conceptMap
        case None =>
          throw ResourceNotFound("Local terminology not found.", s"Local terminology with id $terminologyId not found.")
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
  override def getConceptMap(terminologyId: String, conceptMapId: String): Future[TerminologyConceptMap] = {
    Future {
      val localTerminologyFile = FileOperations.getFileIfExists(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_JSON)
      val localTerminologies = FileOperations.readJsonContent(localTerminologyFile, classOf[LocalTerminology])
      localTerminologies.find(_.id == terminologyId) match {
        case Some(terminology) =>
          terminology.conceptMaps.find(_.id == conceptMapId) match {
            case Some(conceptMap) =>
              conceptMap
            case None =>
              throw ResourceNotFound("Local terminology concept map not found.", s"Local terminology concept map with id $conceptMapId not found.")
          }
        case None =>
          throw ResourceNotFound("Local terminology not found.", s"Local terminology with id $terminologyId not found.")
      }
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
      val localTerminologyFile = FileOperations.getFileIfExists(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_JSON)
      val localTerminology = FileOperations.readJsonContent(localTerminologyFile, classOf[LocalTerminology])
      localTerminology.find(_.id == terminologyId) match {
        case Some(terminology) =>
          val terminologyIdFolder = FileOperations.getFileIfExists(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_FOLDER + File.separator + terminology.name)
          // check if concept map id exists in json file
          if (!terminology.conceptMaps.exists(_.id == conceptMapId)) {
            throw ResourceNotFound("Local terminology concept map not found.", s"Local terminology concept map with id $conceptMapId not found.")
          }
          terminology.conceptMaps.find(_.id == conceptMapId) match {
            case Some(foundConceptMap) =>
              // check if concept map file changed and already exists
              val newConceptMapFile = new File(terminologyIdFolder, conceptMap.name)
              if (foundConceptMap.name != conceptMap.name && newConceptMapFile.exists()) {
                throw AlreadyExists("Local terminology concept map file already exists.", s"File ${conceptMap.name} already exists.")
              }
              // update concept map file
              val newConceptMaps = terminology.conceptMaps.map(cm =>
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
              // rename concept map file
              if (foundConceptMap.name != conceptMap.name) {
                val oldConceptMapFile = new File(terminologyIdFolder, foundConceptMap.name)
                oldConceptMapFile.renameTo(newConceptMapFile)
              }
              conceptMap
            case None =>
              throw ResourceNotFound("Local terminology concept map not found.", s"Local terminology concept map with id $conceptMapId not found.")
          }

        case None =>
          throw ResourceNotFound("Local terminology not found.", s"Local terminology with id $terminologyId not found.")
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
      val localTerminologyFile = FileOperations.getFileIfExists(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_JSON)
      val localTerminology = FileOperations.readJsonContent(localTerminologyFile, classOf[LocalTerminology])
      localTerminology.find(_.id == terminologyId) match {
        case Some(terminology) =>
          val terminologyIdFolder = FileOperations.getFileIfExists(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_FOLDER + File.separator + terminology.name)
          // check if concept map id exists in json file
          if (!terminology.conceptMaps.exists(_.id == conceptMapId)) {
            throw ResourceNotFound("Local terminology concept map not found.", s"Local terminology concept map with id $conceptMapId not found.")
          }
          // remove concept map from json file
          val newConceptMaps = terminology.conceptMaps.filterNot(_.id == conceptMapId)
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
          val conceptMapFile = new File(terminologyIdFolder, terminology.conceptMaps.find(_.id == conceptMapId).get.name)
          conceptMapFile.delete()
        case None =>
          throw ResourceNotFound("Local terminology not found.", s"Local terminology with id $terminologyId not found.")
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
    Future {
      val localTerminologyFile = FileOperations.getFileIfExists(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_JSON)
      val localTerminology = FileOperations.readJsonContent(localTerminologyFile, classOf[LocalTerminology])
      localTerminology.find(_.id == terminologyId) match {
        case Some(terminology) =>
          val terminologyIdFolder = FileOperations.getFileIfExists(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_FOLDER + File.separator + terminology.name)
          // check if concept map id exists in json file
          if (!terminology.conceptMaps.exists(_.id == conceptMapId)) {
            throw ResourceNotFound("Local terminology concept map not found.", s"Local terminology concept map with id $conceptMapId not found.")
          }
          // save concept map file
          val conceptMapFile = FileOperations.getFileIfExists(terminologyIdFolder + File.separator + terminology.conceptMaps.find(_.id == conceptMapId).get.name)
          FileOperations.saveFileContent(conceptMapFile, content)
        case None =>
          throw ResourceNotFound("Local terminology not found.", s"Local terminology with id $terminologyId not found.")
      }
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
      val localTerminologyFile = FileOperations.getFileIfExists(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_JSON)
      val localTerminology = FileOperations.readJsonContent(localTerminologyFile, classOf[LocalTerminology])
      localTerminology.find(_.id == terminologyId) match {
        case Some(terminology) =>
          val terminologyIdFolder = FileOperations.getFileIfExists(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_FOLDER + File.separator + terminology.name)
          // check if concept map id exists in json file
          if (!terminology.conceptMaps.exists(_.id == conceptMapId)) {
            throw ResourceNotFound("Local terminology concept map not found.", s"Local terminology concept map with id $conceptMapId not found.")
          }
          // get concept map file
          val conceptMapFile = FileOperations.getFileIfExists(terminologyIdFolder + File.separator + terminology.conceptMaps.find(_.id == conceptMapId).get.name)
          FileIO.fromPath(conceptMapFile.toPath)
        case None =>
          throw ResourceNotFound("Local terminology not found.", s"Local terminology with id $terminologyId not found.")
      }
    }
  }
}


