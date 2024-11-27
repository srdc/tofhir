package io.tofhir.server.repository.terminology.conceptmap

import akka.stream.scaladsl.{Concat, FileIO, Framing, Source}
import akka.util.ByteString
import io.onfhir.definitions.common.model.Json4sSupport.formats
import io.tofhir.engine.Execution.actorSystem
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.common.model.{AlreadyExists, BadRequest, InternalError, ResourceNotFound}
import io.tofhir.server.model.TerminologySystem.TerminologyConceptMap
import io.tofhir.server.model._
import io.tofhir.server.model.csv.CsvHeader
import io.tofhir.server.repository.terminology.TerminologySystemFolderRepository.getTerminologySystemsJsonPath
import io.tofhir.server.util.{CsvUtil, FileOperations}
import org.json4s.jackson.Serialization.writePretty

import java.io.{File, FileWriter}
import scala.concurrent.Future

class ConceptMapFolderRepository(terminologySystemFolderPath: String) extends IConceptMapRepository {

  /**
   * Retrieve the concept maps within a terminology
   * @return Seq[TerminologyConceptMap]
   */
  override def getConceptMaps(terminologyId: String): Future[Seq[TerminologyConceptMap]] = {
    Future {
      findLocalTerminologyById(terminologyId).conceptMaps
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
  override def getConceptMap(terminologyId: String, conceptMapId: String): Future[TerminologyConceptMap] = {
    Future {
      findConceptMapById(terminologyId, conceptMapId)
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
   * Update the concept map header by its id
   * @param terminologyId terminology id the concept map belongs to
   * @param conceptMapId concept map id e.g. icd9-to-icd10.csv
   * @param headers new headers to update
   * @return
   */
  def updateConceptMapHeader(terminologyId: String, conceptMapId: String, headers: Seq[CsvHeader]): Future[Unit] = {
    val conceptMap = findConceptMapById(terminologyId, conceptMapId)
    // get file and update headers
    val conceptMapFile = FileUtils.getPath(terminologySystemFolderPath, terminologyId, conceptMap.id).toFile
    CsvUtil.writeCsvHeaders(conceptMapFile, headers)
  }


  /**
   * Retrieve and save the content of a concept map csv file within a terminology
   *
   * @param terminologyId id of the terminology
   * @param conceptMapId  id of the concept map
   * @param content       content of the csv file
   * @return
   */
  override def saveConceptMapContent(terminologyId: String, conceptMapId: String, content: Source[ByteString, Any], pageNumber: Int, pageSize: Int): Future[Long] = {
    val conceptMap = findConceptMapById(terminologyId, conceptMapId)
    // save concept map file
    val conceptMapFile = FileUtils.getPath(terminologySystemFolderPath, terminologyId, conceptMap.id).toFile
    CsvUtil.writeCsvAndReturnRowNumber(conceptMapFile, content, pageNumber, pageSize)
  }

  /**
   * Retrieve the content of a concept map csv file within a terminology
   *
   * @param terminologyId id of the terminology
   * @param conceptMapId  id of the concept map
   * @return content of the csv file
   */
  override def getConceptMapContent(terminologyId: String, conceptMapId: String, pageNumber: Int, pageSize: Int): Future[(Source[ByteString, Any], Long)] = {
    val conceptMap = findConceptMapById(terminologyId, conceptMapId)
    // get concept map file
    val file = FileUtils.getPath(terminologySystemFolderPath, terminologyId, conceptMap.id).toFile
    CsvUtil.getPaginatedCsvContent(file, pageNumber, pageSize)
  }

  /**
   * Upload the concept map to the repository
   * @param terminologyId id of the terminology
   * @param conceptMapId  id of the concept map
   * @param content       content of the csv file
   * @return
   */
  override def uploadConceptMap(terminologyId: String, conceptMapId: String, content: Source[ByteString, Any]): Future[Unit] = {
    val conceptMap = findConceptMapById(terminologyId, conceptMapId)
    // save concept map file
    val conceptMapFile = FileUtils.getPath(terminologySystemFolderPath, terminologyId, conceptMap.id).toFile
    CsvUtil.saveFileContent(conceptMapFile, content)
  }

  /**
   * Download the content of a concept map csv file within a terminology
   * @param terminologyId id of the terminology
   * @param conceptMapId  id of the concept map
   * @return content of the csv file
   */
  override def downloadConceptMap(terminologyId: String, conceptMapId: String): Future[Source[ByteString, Any]] = {
    Future {
      val conceptMap = findConceptMapById(terminologyId, conceptMapId)
      // get concept map file
      val conceptMapFile = FileUtils.getPath(terminologySystemFolderPath, terminologyId, conceptMap.id)
      FileIO.fromPath(conceptMapFile)
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
   * Find concept map by id from a local terminology
   * If either terminology or concept map not found, throw ResourceNotFound
   * @param terminologyId id of the terminology
   * @param conceptMapId concept map id to find inside the terminology
   * @return
   */
  private def findConceptMapById(terminologyId: String, conceptMapId: String): TerminologyConceptMap = {
    val localTerminology = findLocalTerminologyById(terminologyId)
    localTerminology.conceptMaps.find(_.id == conceptMapId) match {
      case Some(cs) => cs
      case None => throw ResourceNotFound("Local terminology concept map not found.", s"Local terminology concept map with id $conceptMapId not found.")
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


