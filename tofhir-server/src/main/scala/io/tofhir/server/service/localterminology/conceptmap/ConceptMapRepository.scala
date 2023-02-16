package io.tofhir.server.service.localterminology.conceptmap

import akka.stream.scaladsl.{FileIO, Source}
import akka.util.ByteString
import io.onfhir.util.JsonFormatter._
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.model.{AlreadyExists, BadRequest, LocalTerminology, ResourceNotFound, TerminologyConceptMap}
import io.tofhir.server.service.localterminology.LocalTerminologyFolderRepository.{TERMINOLOGY_FOLDER, TERMINOLOGY_JSON}
import io.tofhir.server.util.FileOperations
import org.json4s.jackson.Serialization.writePretty

import java.io.{File, FileWriter}
import scala.:+
import scala.collection.mutable
import scala.concurrent.Future

class ConceptMapRepository(localTerminologyRepositoryRoot: String) extends IConceptMapRepository {

  // terminology id -> concept map id -> concept map
  private val conceptMaps: mutable.Map[String, mutable.Map[String, TerminologyConceptMap]] = initMap()

  /**
   * Retrieve the concept maps within a terminology
   * @return Seq[TerminologyConceptMap]
   */
  override def getConceptMaps(terminologyId: String): Future[Seq[TerminologyConceptMap]] = {
    Future {
      conceptMaps(terminologyId).values.toSeq
    }
  }

  /**
   * Create a new concept map within a terminology
   *
   * @return TerminologyConceptMap
   */
  override def createConceptMap(terminologyId: String, conceptMap: TerminologyConceptMap): Future[TerminologyConceptMap] = {
    Future {
      val localTerminologyFile = FileUtils.getPath(localTerminologyRepositoryRoot, TERMINOLOGY_JSON).toFile
      val localTerminology = FileOperations.readJsonContent[LocalTerminology](localTerminologyFile)
      // check if concept map id exists
      if (conceptMaps.contains(terminologyId) && conceptMaps(terminologyId).contains(conceptMap.id)) {
        throw AlreadyExists("Local terminology concept map id already exists.", s"Id ${conceptMap.id} already exists.")
      }
      // update map
      conceptMaps.getOrElseUpdate(terminologyId, mutable.Map.empty).put(conceptMap.id, conceptMap)
      // update json file
      val newConceptMaps = conceptMaps(terminologyId).values.toSeq :+ conceptMap
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
      val conceptMapFile = FileUtils.getPath(localTerminologyRepositoryRoot, TERMINOLOGY_FOLDER, terminologyId, conceptMap.id).toFile
      conceptMapFile.createNewFile()
      conceptMap
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
      conceptMaps(terminologyId).get(conceptMapId)
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
      val localTerminology = FileOperations.readJsonContent[LocalTerminology](localTerminologyFile)
      // check if concept map id exists in json file
      if (!conceptMaps(terminologyId).contains(conceptMapId)) {
        throw ResourceNotFound("Local terminology concept map not found.", s"Local terminology concept map with id $conceptMapId not found.")
      }
      // update metadata
      val newConceptMaps = conceptMaps(terminologyId).values.toSeq.map(cm =>
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
      // update cache
      conceptMaps(terminologyId).put(conceptMapId, conceptMap)
      conceptMap
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
      val localTerminology = FileOperations.readJsonContent[LocalTerminology](localTerminologyFile)
      // check if concept map id exists in json file
      if (!conceptMaps(terminologyId).contains(conceptMapId)) {
        throw ResourceNotFound("Local terminology concept map not found.", s"Local terminology concept map with id $conceptMapId not found.")
      }
      // remove concept map from json file
      val newConceptMaps = conceptMaps(terminologyId).values.toSeq.filterNot(_.id == conceptMapId)
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
      val conceptMapFile = FileUtils.getPath(localTerminologyRepositoryRoot, TERMINOLOGY_FOLDER, terminologyId, conceptMaps(terminologyId)(conceptMapId).id).toFile
      conceptMapFile.delete()
      // remove from cache
      conceptMaps.remove(conceptMapId)
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
      if (!conceptMaps(terminologyId).contains(conceptMapId)) {
        throw ResourceNotFound("Local terminology concept map not found.", s"Local terminology concept map with id $conceptMapId not found.")
      }
      // save concept map file
      val conceptMapFile = FileUtils.getPath(localTerminologyRepositoryRoot, TERMINOLOGY_FOLDER, terminologyId, conceptMaps(terminologyId)(conceptMapId).id).toFile
      FileOperations.saveFileContent(conceptMapFile, content)
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
      if (!conceptMaps(terminologyId).contains(conceptMapId)) {
        throw ResourceNotFound("Local terminology concept map not found.", s"Local terminology concept map with id $conceptMapId not found.")
      }
      // get concept map file
      val conceptMapFile = FileUtils.getPath(localTerminologyRepositoryRoot, TERMINOLOGY_FOLDER, terminologyId, conceptMaps(terminologyId)(conceptMapId).id)
      FileIO.fromPath(conceptMapFile)
    }
  }

  /**
   * Initialize the cache
   * @return
   */
  private def initMap(): mutable.Map[String, mutable.Map[String, TerminologyConceptMap]] = {
    val localTerminologyFile = FileUtils.getPath(localTerminologyRepositoryRoot, TERMINOLOGY_JSON).toFile
    val localTerminologies = FileOperations.readJsonContent[LocalTerminology](localTerminologyFile)
    val map = mutable.Map[String, mutable.Map[String, TerminologyConceptMap]]()
    localTerminologies.foreach(t => {
      val conceptMaps = mutable.Map[String, TerminologyConceptMap]()
      t.conceptMaps.foreach(cm => {
        conceptMaps.put(cm.id, cm)
      })
      map.put(t.id, conceptMaps)
    })
    map
  }
}


