package io.tofhir.server.service.localterminology

import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.server.model.{AlreadyExists, BadRequest, LocalTerminology, ResourceNotFound}
import io.tofhir.server.service.localterminology.LocalTerminologyFolderRepository.{TERMINOLOGY_FOLDER, TERMINOLOGY_JSON}
import io.tofhir.server.util.FileOperations
import org.apache.commons.io.FileUtils

import java.io.File
import scala.concurrent.Future

/**
 * Folder/Directory based local terminology repository implementation.
 *
 * @param localTerminologyRepositoryRoot root folder path to the local terminology repository
 */
class LocalTerminologyFolderRepository(localTerminologyRepositoryRoot: String) extends ILocalTerminologyRepository {

  // load local terminology services from json file on startup
  var localTerminologies: Seq[LocalTerminology] = readLocalTerminologyFile()

  /**
   * Retrieve the metadata of all LocalTerminology
   * @return Seq[LocalTerminology]
   */
  override def getAllLocalTerminologyMetadata: Future[Seq[LocalTerminology]] = {
    Future {
      this.localTerminologies
    }
  }

  /**
   * Create a new LocalTerminology
   * @return LocalTerminology
   */
  override def createTerminologyService(terminology: LocalTerminology): Future[LocalTerminology] = {
    Future {
      this.validate(terminology)
      val localTerminologyFile = FileOperations.getFileIfExists(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_JSON)
      val terminologyFolder = new File(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_FOLDER + File.separator + terminology.name)
      if (terminologyFolder.exists()) {
        throw AlreadyExists("Local terminology folder already exists.", s"Folder ${terminology.name} already exists.")
      }
      // check if id exists
      if (this.localTerminologies.exists(_.id == terminology.id)) {
        throw AlreadyExists("Local terminology id already exists.", s"Id ${terminology.id} already exists.")
      }
      this.localTerminologies = this.localTerminologies :+ terminology
      FileOperations.writeJsonContent(localTerminologyFile, this.localTerminologies)
      terminologyFolder.mkdirs() // create folder
      this.createConceptMapAndCodeSystemFiles(terminology) // create concept maps/code systems files
      terminology
    }
  }

  /**
   * Retrieve a LocalTerminologyService
   * @param id id of the LocalTerminologyService
   * @return LocalTerminologyService
   */
  override def retrieveTerminology(id: String): Future[LocalTerminology] = {
    Future {
      this.localTerminologies.find(_.id == id) match {
        case Some(terminology) => terminology
        case None => throw ResourceNotFound("Local terminology not found.", s"Local terminology with id $id not found.")
      }
    }
  }

  /**
   * Update a LocalTerminologyService
   * @param id id of the LocalTerminologyService
   * @param terminology LocalTerminologyService to update
   * @return updated LocalTerminologyService
   */
  override def updateTerminologyService(id: String, terminology: LocalTerminology): Future[LocalTerminology] = {
    Future {
      //cross check ids
      if (id != terminology.id) {
        throw BadRequest("Local terminology id does not match.", s"Id $id does not match with the id in the body ${terminology.id}.")
      }
      this.validate(terminology)
      val localTerminologyFile = FileOperations.getFileIfExists(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_JSON)
      // find the terminology service
      this.localTerminologies.find(_.id == id) match {
        case Some(foundTerminology) =>
          // check if folders exists
          val terminologyFolder = FileOperations.getFileIfExists(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_FOLDER + File.separator + foundTerminology.name)
          val newTerminologyFolder = new File(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_FOLDER + File.separator + terminology.name)
          if (terminology.name != foundTerminology.name && newTerminologyFolder.exists()) {
            throw AlreadyExists("Local terminology folder already exists.", s"Folder ${terminology.name} already exists.")
          }
          // update the terminology service json
          this.localTerminologies = this.localTerminologies.map {
            case t if t.id == id => terminology
            case t => t
          }
          FileOperations.writeJsonContent(localTerminologyFile, this.localTerminologies)
          //update folder name if changed
          if (terminology.name != foundTerminology.name) {
            terminologyFolder.renameTo(newTerminologyFolder)
          }
          this.updateConceptMapAndCodeSystemFiles(foundTerminology, terminology) // update concept maps/code systems files
          terminology
        case None => throw ResourceNotFound("Local terminology not found.", s"Local terminology with id $id not found.")
      }
    }
  }

  /**
   * Remove a LocalTerminologyService
   * @param id id of the LocalTerminologyService
   * @return
   */
  override def removeTerminology(id: String): Future[Unit] = {
    Future {
      val localTerminologyFile = FileOperations.getFileIfExists(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_JSON)
      // find the terminology service
      this.localTerminologies.find(_.id == id) match {
        case Some(foundTerminology) =>
          // check if folders exists
          val terminologyFolder = FileOperations.getFileIfExists(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_FOLDER + File.separator + foundTerminology.name)
          // delete the terminology
          this.localTerminologies = this.localTerminologies.filterNot(_.id == id)
          FileOperations.writeJsonContent(localTerminologyFile, this.localTerminologies)
          //delete the terminology folder
          FileUtils.deleteDirectory(terminologyFolder)
        case None => throw ResourceNotFound("Local terminology not found.", s"Local terminology with id $id not found.")
      }
    }
  }

  /**
   * Create concept map and code system files in the terminology folder according to the new terminology
   * @param terminology LocalTerminologyService
   * @return
   */
  private def createConceptMapAndCodeSystemFiles(terminology: LocalTerminology): Future[Unit] = {
    Future {
      terminology.conceptMaps.foreach(conceptMap => {
        val file = new File(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_FOLDER + File.separator + terminology.name + File.separator + conceptMap.name)
        file.createNewFile()
      })
      terminology.codeSystems.foreach(codeSystem => {
        val file = new File(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_FOLDER + File.separator + terminology.name + File.separator + codeSystem.name)
        file.createNewFile()
      })
    }
  }

  /**
   * Create/delete concept map and code system files in the terminology folder according to the new terminology
   * @param terminology LocalTerminologyService
   * @return
   */
  private def updateConceptMapAndCodeSystemFiles(currentTerminology: LocalTerminology, terminology: LocalTerminology): Future[Unit] = {
    Future {
      // find intersection ids of concept map and code system in local terminologies
      val currentConceptMapIds = currentTerminology.conceptMaps.map(_.id)
      val currentCodeSystemIds = currentTerminology.codeSystems.map(_.id)
      val newConceptMapIds = terminology.conceptMaps.map(_.id)
      val newCodeSystemIds = terminology.codeSystems.map(_.id)
      // find intersections
      val conceptMapIdsIntersection = currentConceptMapIds.intersect(newConceptMapIds)
      val codeSystemIdsIntersection = currentCodeSystemIds.intersect(newCodeSystemIds)
      // update file names if changed
      conceptMapIdsIntersection.foreach { id =>
        val currentConceptMap = currentTerminology.conceptMaps.find(_.id == id).get
        val newConceptMap = terminology.conceptMaps.find(_.id == id).get
        if (currentConceptMap.name != newConceptMap.name) {
          val currentFile = FileOperations.getFileIfExists(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_FOLDER + File.separator + currentTerminology.name + File.separator + currentConceptMap.name)
          val newFile = new File(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_FOLDER + File.separator + terminology.name + File.separator + newConceptMap.name)
          currentFile.renameTo(newFile)
        }
      }
      codeSystemIdsIntersection.foreach { id =>
        val currentCodeSystem = currentTerminology.codeSystems.find(_.id == id).get
        val newCodeSystem = terminology.codeSystems.find(_.id == id).get
        if (currentCodeSystem.name != newCodeSystem.name) {
          val currentFile = FileOperations.getFileIfExists(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_FOLDER + File.separator + currentTerminology.name + File.separator + currentCodeSystem.name)
          val newFile = new File(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_FOLDER + File.separator + terminology.name + File.separator + newCodeSystem.name)
          currentFile.renameTo(newFile)
        }
      }

      val conceptMapIdsToDelete = currentConceptMapIds.diff(newConceptMapIds)
      val conceptMapIdsToAdd = newConceptMapIds.diff(currentConceptMapIds)
      val codeSystemIdsToDelete = currentCodeSystemIds.diff(newCodeSystemIds)
      val codeSystemIdsToAdd = newCodeSystemIds.diff(currentCodeSystemIds)
      // delete concept maps
      conceptMapIdsToDelete.foreach { id =>
        val conceptMap = currentTerminology.conceptMaps.find(_.id == id).get
        val file = FileOperations.getFileIfExists(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_FOLDER + File.separator + currentTerminology.name + File.separator + conceptMap.name)
        file.delete()
      }
      // delete code systems
      codeSystemIdsToDelete.foreach { id =>
        val codeSystem = currentTerminology.codeSystems.find(_.id == id).get
        val file = FileOperations.getFileIfExists(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_FOLDER + File.separator + currentTerminology.name + File.separator + codeSystem.name)
        file.delete()
      }
      // add concept maps
      conceptMapIdsToAdd.foreach { id =>
        val conceptMap = terminology.conceptMaps.find(_.id == id).get
        val file = new File(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_FOLDER + File.separator + terminology.name + File.separator + conceptMap.name)
        file.createNewFile()
      }
      // add code systems
      codeSystemIdsToAdd.foreach { id =>
        val codeSystem = terminology.codeSystems.find(_.id == id).get
        val file = new File(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_FOLDER + File.separator + terminology.name + File.separator + codeSystem.name)
        file.createNewFile()
      }
    }
  }

  /**
   * Read local terminology file/folder if exists, otherwise create a new one
   * @return
   */
  private def readLocalTerminologyFile(): Seq[LocalTerminology] = {
    val localTerminologyFolder = new File(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_FOLDER)
    if (!localTerminologyFolder.exists()) {
      localTerminologyFolder.mkdirs()
    }
    val localTerminologyFile = new File(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_JSON)
    if (!localTerminologyFile.exists()) {
      localTerminologyFile.createNewFile()
      FileOperations.writeJsonContent(localTerminologyFile, Seq.empty[LocalTerminology])
    }
    val localTerminologies = FileOperations.readJsonContent(localTerminologyFile, classOf[LocalTerminology])
    localTerminologies
  }

  /**
   * Validate terminology
   * @param terminology terminology to validate
   */
  private def validate(terminology: LocalTerminology): Unit = {
    if (terminology.name == null || terminology.name.isEmpty) {
      throw BadRequest("Terminology name cannot be empty", "")
    }
    terminology.conceptMaps.foreach(conceptMap => {
      if (conceptMap.name == null || conceptMap.name.isEmpty) {
        throw BadRequest("Concept map name cannot be empty", "")
      }
    })
    terminology.codeSystems.foreach(codeSystem => {
      if (codeSystem.name == null || codeSystem.name.isEmpty) {
        throw BadRequest("Code system name cannot be empty", "")
      }
    })
    // combine and check if concept map/code system name is unique
    val names = terminology.conceptMaps.map(_.name) ++ terminology.codeSystems.map(_.name)
    if (names.distinct.length != names.length) {
      throw BadRequest("The same name occurs more than once in the data", "")
    }
  }
}

object LocalTerminologyFolderRepository {
  val TERMINOLOGY_FOLDER = "terminology-services"
  val TERMINOLOGY_JSON = TERMINOLOGY_FOLDER + File.separator + "terminology-services.json"
}
