package io.tofhir.server.service.localterminology

import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.model.{AlreadyExists, BadRequest, LocalTerminology, ResourceNotFound}
import io.tofhir.server.service.localterminology.LocalTerminologyFolderRepository.{TERMINOLOGY_FOLDER, TERMINOLOGY_JSON}
import io.tofhir.server.util.FileOperations

import java.io.File
import scala.collection.mutable
import scala.concurrent.Future

/**
 * Folder/Directory based local terminology repository implementation.
 *
 * @param localTerminologyRepositoryRoot root folder path to the local terminology repository
 */
class LocalTerminologyFolderRepository(localTerminologyRepositoryRoot: String) extends ILocalTerminologyRepository {
  // local terminology id -> local terminology
  private val localTerminologies: mutable.Map[String, LocalTerminology] = initMap(localTerminologyRepositoryRoot)

  /**
   * Retrieve the metadata of all LocalTerminology
   * @return Seq[LocalTerminology]
   */
  override def getAllLocalTerminologyMetadata: Future[Seq[LocalTerminology]] = {
    Future {
      this.localTerminologies.values.map(_.copy(conceptMaps = Seq.empty, codeSystems = Seq.empty)).toSeq
    }
  }

  /**
   * Create a new LocalTerminology
   * @return LocalTerminology
   */
  override def createTerminologyService(terminology: LocalTerminology): Future[LocalTerminology] = {
    Future {
      this.validate(terminology)
      // check if id exists
      if (this.localTerminologies.contains(terminology.id)) {
        throw AlreadyExists("Local terminology id already exists.", s"Id ${terminology.id} already exists.")
      }
      val terminologyFolder = FileUtils.getPath(localTerminologyRepositoryRoot, TERMINOLOGY_FOLDER, terminology.id).toFile
      // add new terminology to the list and write to file
      val updatedLocalTerminologies = this.localTerminologies.values.toSeq :+ terminology
      this.updateTerminologiesMetadata(updatedLocalTerminologies)
      // create folder
      terminologyFolder.mkdirs()
      this.createConceptMapAndCodeSystemFiles(terminology) // create concept maps/code systems files
      // add to map
      this.localTerminologies.put(terminology.id, terminology)
      terminology
    }
  }

  /**
   * Retrieve a LocalTerminologyService
   * @param id id of the LocalTerminologyService
   * @return LocalTerminologyService
   */
  override def retrieveTerminology(id: String): Future[Option[LocalTerminology]] = {
    Future {
      this.localTerminologies.get(id)
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
      // find the terminology service
      this.localTerminologies.get(id) match {
        case Some(foundTerminology) =>
          // update the terminology service metadata
          val updatedLocalTerminologies = this.localTerminologies.values.map {
            case t if t.id == id => terminology
            case t => t
          }.toSeq
          this.updateTerminologiesMetadata(updatedLocalTerminologies)
          // update concept maps/code systems files
          this.updateConceptMapAndCodeSystemFiles(foundTerminology, terminology)
          // update the terminology service in the map
          this.localTerminologies.put(id, terminology)
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
      // find the terminology service
      this.localTerminologies.get(id) match {
        case Some(foundTerminology) =>
          // delete the terminology metadata
          val updatedTerminologies = this.localTerminologies.values.toSeq.filterNot(_.id == id)
          this.updateTerminologiesMetadata(updatedTerminologies)
          //delete the terminology folder
          val terminologyFolder = FileUtils.getPath(localTerminologyRepositoryRoot, TERMINOLOGY_FOLDER, foundTerminology.id).toFile
          org.apache.commons.io.FileUtils.deleteDirectory(terminologyFolder)
          // delete the terminology from the map
          this.localTerminologies.remove(id)
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
        val file = FileUtils.getPath(localTerminologyRepositoryRoot, TERMINOLOGY_FOLDER, terminology.id, conceptMap.id).toFile
        file.createNewFile()
      })
      terminology.codeSystems.foreach(codeSystem => {
        val file = FileUtils.getPath(localTerminologyRepositoryRoot, TERMINOLOGY_FOLDER, terminology.id, codeSystem.id).toFile
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
      // find ids to delete and add
      val conceptMapIdsToDelete = currentConceptMapIds.diff(newConceptMapIds)
      val conceptMapIdsToAdd = newConceptMapIds.diff(currentConceptMapIds)
      val codeSystemIdsToDelete = currentCodeSystemIds.diff(newCodeSystemIds)
      val codeSystemIdsToAdd = newCodeSystemIds.diff(currentCodeSystemIds)
      // delete concept maps
      conceptMapIdsToDelete.foreach { id =>
        val conceptMap = currentTerminology.conceptMaps.find(_.id == id).get
        val file = FileUtils.getPath(localTerminologyRepositoryRoot, TERMINOLOGY_FOLDER, currentTerminology.id, conceptMap.id).toFile
        file.delete()
      }
      // delete code systems
      codeSystemIdsToDelete.foreach { id =>
        val codeSystem = currentTerminology.codeSystems.find(_.id == id).get
        val file = FileUtils.getPath(localTerminologyRepositoryRoot, TERMINOLOGY_FOLDER, currentTerminology.id, codeSystem.id).toFile
        file.delete()
      }
      // add concept maps
      conceptMapIdsToAdd.foreach { id =>
        val conceptMap = terminology.conceptMaps.find(_.id == id).get
        val file = FileUtils.getPath(localTerminologyRepositoryRoot, TERMINOLOGY_FOLDER, currentTerminology.id, conceptMap.id).toFile
        file.createNewFile()
      }
      // add code systems
      codeSystemIdsToAdd.foreach { id =>
        val codeSystem = terminology.codeSystems.find(_.id == id).get
        val file = FileUtils.getPath(localTerminologyRepositoryRoot, TERMINOLOGY_FOLDER, currentTerminology.id, codeSystem.id).toFile
        file.createNewFile()
      }
    }
  }

  /**
   * Read local terminology file/folder if exists, otherwise create a new one
   * @return
   */
  private def readLocalTerminologyFile(): Seq[LocalTerminology] = {
    val localTerminologyFolder = FileUtils.getPath(localTerminologyRepositoryRoot, TERMINOLOGY_FOLDER).toFile
    if (!localTerminologyFolder.exists()) {
      localTerminologyFolder.mkdirs()
    }
    val localTerminologyFile = FileUtils.getPath(localTerminologyRepositoryRoot, TERMINOLOGY_JSON).toFile
    if (!localTerminologyFile.exists()) {
      localTerminologyFile.createNewFile()
      FileOperations.writeJsonContent(localTerminologyFile, Seq.empty[LocalTerminology])
    }
    val localTerminologies = FileOperations.readJsonContent[LocalTerminology](localTerminologyFile)
    localTerminologies
  }

  /**
   * Update local terminology file
   * @param localTerminologyRepositoryRoot
   * @return
   */
  private def initMap(localTerminologyRepositoryRoot: String): mutable.Map[String, LocalTerminology] = {
    val localTerminologies = readLocalTerminologyFile()
    val map = mutable.Map[String, LocalTerminology]()
    localTerminologies.foreach(terminology => {
      map.put(terminology.id, terminology)
    })
    map
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

  private def updateTerminologiesMetadata(terminologies: Seq[LocalTerminology]): Unit = {
    val localTerminologyFile = FileUtils.getPath(localTerminologyRepositoryRoot, TERMINOLOGY_JSON).toFile
    FileOperations.writeJsonContent(localTerminologyFile, terminologies)
  }

}

object LocalTerminologyFolderRepository {
  val TERMINOLOGY_FOLDER = "terminology-services"
  val TERMINOLOGY_JSON = TERMINOLOGY_FOLDER + File.separator + "terminology-services.json"
}
