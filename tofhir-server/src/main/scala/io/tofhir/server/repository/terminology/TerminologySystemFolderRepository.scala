package io.tofhir.server.repository.terminology

import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.repository.ICachedRepository
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.common.model.{AlreadyExists, BadRequest, ResourceNotFound}
import io.tofhir.server.model.TerminologySystem
import io.tofhir.server.repository.terminology.TerminologySystemFolderRepository.getTerminologySystemsJsonPath
import io.tofhir.server.util.FileOperations

import java.io.File
import scala.collection.mutable
import scala.concurrent.Future

/**
 * Folder/Directory based terminology system repository implementation.
 */
class TerminologySystemFolderRepository(terminologySystemsFolderPath: String) extends ITerminologySystemRepository with ICachedRepository {

  // terminology system id -> TerminologySystem
  private val terminologySystemMap: mutable.Map[String, TerminologySystem] = mutable.Map.empty[String, TerminologySystem]
  // Initialize the map for the first time
  initMap()

  /**
   * Retrieve the metadata of all TerminologySystems
   *
   * @return Seq[TerminologySystem]
   */
  override def getTerminologySystemsMetadata: Future[Seq[TerminologySystem]] = {
    Future {
      this.terminologySystemMap.values.map(_.copy(conceptMaps = Seq.empty, codeSystems = Seq.empty)).toSeq
    }
  }

  /**
   * Create a new TerminologySystem by creating its folder and files in it.
   *
   * @return TerminologySystem
   */
  override def createTerminologySystem(terminologySystem: TerminologySystem): Future[TerminologySystem] = {
    Future {
      this.validate(terminologySystem)
      // check if id exists
      if (this.terminologySystemMap.contains(terminologySystem.id)) {
        throw AlreadyExists("TerminologySystem already exists.", s"Id ${terminologySystem.id} already exists.")
      }
      val terminologySystemFolder = FileUtils.getPath(terminologySystemsFolderPath, terminologySystem.id).toFile
      // Update the internal JSON file that we use as our database
      val terminologySystemSeq = this.terminologySystemMap.values.toSeq :+ terminologySystem
      this.updateTerminologySystemsDBFile(terminologySystemSeq)

      terminologySystemFolder.mkdirs() // create its folder
      this.createConceptMapAndCodeSystemFiles(terminologySystem) // create concept maps/code systems files
      // Update the map that we use as a cache
      this.terminologySystemMap.put(terminologySystem.id, terminologySystem)
      terminologySystem
    }
  }

  /**
   * Retrieve a TerminologySystem
   *
   * @param id id of the TerminologySystem
   * @return TerminologySystem
   */
  override def getTerminologySystem(id: String): Future[Option[TerminologySystem]] = {
    Future {
      this.terminologySystemMap.get(id)
    }
  }

  /**
   * Update a TerminologySystem
   *
   * @param id                id of the TerminologySystem
   * @param terminologySystem TerminologySystem to update
   * @return updated TerminologySystem
   */
  override def updateTerminologySystem(id: String, terminologySystem: TerminologySystem): Future[TerminologySystem] = {
    //cross check ids
    if (id != terminologySystem.id) {
      throw BadRequest("Terminology System IDs do not match.", s"Id $id does not match with the id in the body ${terminologySystem.id}.")
    }
    this.validate(terminologySystem)
    // find the terminology system
    this.terminologySystemMap.get(id) match {
      case Some(foundTerminology) =>
        // update the terminology system metadata in the database
        val updatedLocalTerminologies = this.terminologySystemMap.values.map {
          case t if t.id == id => terminologySystem
          case t => t
        }.toSeq
        this.updateTerminologySystemsDBFile(updatedLocalTerminologies)
        // update concept maps/code systems files
        this.updateConceptMapAndCodeSystemFiles(foundTerminology, terminologySystem) map (_ => {
          // update the terminology service in the map
          this.terminologySystemMap.put(id, terminologySystem)
          terminologySystem
        })
      case None => throw ResourceNotFound("Terminology system not found.", s"Terminology system with id $id not found.")
    }
  }

  /**
   * Remove a TerminologySystem
   *
   * @param id id of the TerminologySystem
   * @return
   */
  override def deleteTerminologySystem(id: String): Future[Unit] = {
    Future {
      // find the terminology system from the cache
      this.terminologySystemMap.get(id) match {
        case Some(foundTerminology) =>
          // delete the terminology metadata
          val updatedTerminologies = this.terminologySystemMap.values.toSeq.filterNot(_.id == id)
          this.updateTerminologySystemsDBFile(updatedTerminologies)
          //delete the terminology folder
          val terminologyFolder = FileUtils.getPath(terminologySystemsFolderPath, foundTerminology.id).toFile
          org.apache.commons.io.FileUtils.deleteDirectory(terminologyFolder)
          // remove the terminology from the map
          this.terminologySystemMap.remove(id)
        case None => throw ResourceNotFound("Terminology system not found.", s"Terminology system with id $id not found.")
      }
    }
  }

  /**
   * Create concept map and code system files in the terminology system folder.
   *
   * @param terminologySystem TerminologySystem
   * @return
   */
  private def createConceptMapAndCodeSystemFiles(terminologySystem: TerminologySystem): Future[Unit] = {
    Future {
      terminologySystem.conceptMaps.foreach(conceptMap => {
        val file = FileUtils.getPath(terminologySystemsFolderPath, terminologySystem.id, conceptMap.id).toFile
        FileOperations.writeStringContentToFile(file, TerminologySystemFolderRepository.DEFAULT_CONCEPT_MAP_COLUMNS)
      })
      terminologySystem.codeSystems.foreach(codeSystem => {
        val file = FileUtils.getPath(terminologySystemsFolderPath, terminologySystem.id, codeSystem.id).toFile
        FileOperations.writeStringContentToFile(file, TerminologySystemFolderRepository.DEFAULT_CODE_SYSTEM_COLUMNS)
      })
    }
  }

  /**
   * Create/delete concept map and code system files in the terminology folder according to the new TerminologySystem
   *
   * @param existingTerminologySystem
   * @param newTerminologySystem
   * @return
   */
  private def updateConceptMapAndCodeSystemFiles(existingTerminologySystem: TerminologySystem, newTerminologySystem: TerminologySystem): Future[Unit] = {
    Future {
      // find intersection ids of concept map and code system in local terminologies
      val currentConceptMapIds = existingTerminologySystem.conceptMaps.map(_.id)
      val currentCodeSystemIds = existingTerminologySystem.codeSystems.map(_.id)
      val newConceptMapIds = newTerminologySystem.conceptMaps.map(_.id)
      val newCodeSystemIds = newTerminologySystem.codeSystems.map(_.id)
      // find ids to delete and add
      val conceptMapIdsToDelete = currentConceptMapIds.diff(newConceptMapIds)
      val conceptMapIdsToAdd = newConceptMapIds.diff(currentConceptMapIds)
      val codeSystemIdsToDelete = currentCodeSystemIds.diff(newCodeSystemIds)
      val codeSystemIdsToAdd = newCodeSystemIds.diff(currentCodeSystemIds)
      // delete concept maps
      conceptMapIdsToDelete.foreach { id =>
        val conceptMap = existingTerminologySystem.conceptMaps.find(_.id == id).get
        val file = FileUtils.getPath(terminologySystemsFolderPath, existingTerminologySystem.id, conceptMap.id).toFile
        file.delete()
      }
      // delete code systems
      codeSystemIdsToDelete.foreach { id =>
        val codeSystem = existingTerminologySystem.codeSystems.find(_.id == id).get
        val file = FileUtils.getPath(terminologySystemsFolderPath, existingTerminologySystem.id, codeSystem.id).toFile
        file.delete()
      }
      // add concept maps
      conceptMapIdsToAdd.foreach { id =>
        val conceptMap = newTerminologySystem.conceptMaps.find(_.id == id).get
        val file = FileUtils.getPath(terminologySystemsFolderPath, existingTerminologySystem.id, conceptMap.id).toFile
        FileOperations.writeStringContentToFile(file, TerminologySystemFolderRepository.DEFAULT_CONCEPT_MAP_COLUMNS)
      }
      // add code systems
      codeSystemIdsToAdd.foreach { id =>
        val codeSystem = newTerminologySystem.codeSystems.find(_.id == id).get
        val file = FileUtils.getPath(terminologySystemsFolderPath, existingTerminologySystem.id, codeSystem.id).toFile
        FileOperations.writeStringContentToFile(file, TerminologySystemFolderRepository.DEFAULT_CODE_SYSTEM_COLUMNS)
      }
    }
  }

  /**
   * Read local terminology file/folder if exists, otherwise create a new one
   *
   * @return
   */
  private def readTerminologySystemsDBFile(): Seq[TerminologySystem] = {
    val terminologySystemsFolder = FileUtils.getPath(terminologySystemsFolderPath).toFile
    if (!terminologySystemsFolder.exists()) {
      terminologySystemsFolder.mkdirs()
    }
    val terminologySystemsJsonFile = FileUtils.getPath(getTerminologySystemsJsonPath(terminologySystemsFolderPath)).toFile
    if (!terminologySystemsJsonFile.exists()) {
      terminologySystemsJsonFile.createNewFile()
      FileOperations.writeJsonContent(terminologySystemsJsonFile, Seq.empty[TerminologySystem])
    }
    val availableTerminologySystems = FileOperations.readJsonContent[TerminologySystem](terminologySystemsJsonFile)
    availableTerminologySystems
  }

  /**
   * Initialize the map that we use as cache.
   *
   * @return
   */
  private def initMap(): Unit = {
    val termonologySystems = readTerminologySystemsDBFile()
    termonologySystems.foreach(terminology => {
      this.terminologySystemMap.put(terminology.id, terminology)
    })
  }

  /**
   * Validate the given TerminologySystem
   *
   * @param terminologySystem
   */
  private def validate(terminologySystem: TerminologySystem): Unit = {
    if (terminologySystem.name == null || terminologySystem.name.isEmpty) {
      throw BadRequest("TerminologySystem name cannot be empty", s"TerminologySystem id: ${terminologySystem.id}")
    }
    terminologySystem.conceptMaps.foreach(conceptMap => {
      if (conceptMap.name == null || conceptMap.name.isEmpty) {
        throw BadRequest("Concept map name cannot be empty", s"TerminologySystem id: ${terminologySystem.id}")
      }
    })
    terminologySystem.codeSystems.foreach(codeSystem => {
      if (codeSystem.name == null || codeSystem.name.isEmpty) {
        throw BadRequest("Code system name cannot be empty", s"TerminologySystem id: ${terminologySystem.id}")
      }
    })
    // combine and check if concept map/code system name is unique
    val names = terminologySystem.conceptMaps.map(_.name) ++ terminologySystem.codeSystems.map(_.name)
    if (names.distinct.length != names.length) {
      throw BadRequest("A name cannot be used more than once across concept maps and code systems within a TerminologySystem", s"TerminologySystem id: ${terminologySystem.id}.")
    }
  }

  private def updateTerminologySystemsDBFile(terminologySystems: Seq[TerminologySystem]): Unit = {
    val localTerminologyFile = FileUtils.getPath(getTerminologySystemsJsonPath(terminologySystemsFolderPath)).toFile
    FileOperations.writeJsonContent(localTerminologyFile, terminologySystems)
  }

  /**
   * Reload the terminology systems from the given folder
   *
   * @return
   */
  def invalidate(): Unit = {
    this.terminologySystemMap.clear()
    initMap()
  }
}

object TerminologySystemFolderRepository {
  private val DEFAULT_CONCEPT_MAP_COLUMNS = "source_system,source_code,target_system,target_code"
  private val DEFAULT_CODE_SYSTEM_COLUMNS = "code,display"

  /**
   * Returns the path for the JSON file keeping the terminology systems
   *
   * @param terminologySystemFolder
   * @return
   */
  def getTerminologySystemsJsonPath(terminologySystemFolder: String): String = {
    terminologySystemFolder + File.separator + "terminology-systems.json"
  }
}
