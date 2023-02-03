package io.tofhir.server.service.localterminology

import io.onfhir.util.JsonFormatter._
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.server.model.{AlreadyExists, BadRequest, InternalError, LocalTerminology, ResourceNotFound}
import io.tofhir.server.service.localterminology.LocalTerminologyFolderRepository.{TERMINOLOGY_FOLDER, TERMINOLOGY_JSON}
import io.tofhir.server.util.FileOperations
import org.apache.commons.io.FileUtils
import org.json4s.jackson.Serialization.writePretty

import java.io.{File, FileWriter}
import scala.concurrent.Future

/**
 * Folder/Directory based local terminology repository implementation.
 *
 * @param localTerminologyRepositoryRoot root folder path to the local terminology repository
 */
class LocalTerminologyFolderRepository(localTerminologyRepositoryRoot: String) extends ILocalTerminologyRepository {

  /**
   * Retrieve the metadata of all LocalTerminology
   * @return Seq[LocalTerminology]
   */
  override def getAllLocalTerminologyMetadata: Future[Seq[LocalTerminology]] = {
    Future {
      val localTerminologyFile = FileOperations.getFileIfExists(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_JSON)
      val localTerminology = FileOperations.readJsonContent(localTerminologyFile, classOf[LocalTerminology])
      localTerminology
    }
  }

  /**
   * Create a new LocalTerminology
   * @return LocalTerminology
   */
  override def createTerminologyService(terminology: LocalTerminology): Future[LocalTerminology] = {
    Future {
      val localTerminologyFile = FileOperations.getFileIfExists(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_JSON)
      val terminologyFolder = new File(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_FOLDER + File.separator + terminology.name)
      if (terminologyFolder.exists()) {
        throw AlreadyExists("Local terminology folder already exists.", s"Folder ${terminology.name} already exists.")
      }
      // append to json file
      val localTerminology = FileOperations.readJsonContent(localTerminologyFile, classOf[LocalTerminology])
      // check if id exists in json file
      if (localTerminology.exists(_.id == terminology.id)) {
        throw AlreadyExists("Local terminology id already exists.", s"Id ${terminology.id} already exists.")
      }
      val newLocalTerminology = localTerminology :+ terminology
      val writer = new FileWriter(localTerminologyFile)
      try {
        writer.write(writePretty(newLocalTerminology))
      } finally {
        writer.close()
      }
      terminologyFolder.mkdirs() // create folder
      this.updateConceptMapAndCodeSystemFiles(terminology) // create concept maps
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
      val localTerminologyFile = FileOperations.getFileIfExists(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_JSON)
      val localTerminologies = FileOperations.readJsonContent(localTerminologyFile, classOf[LocalTerminology])
      localTerminologies.find(_.id == id) match {
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
      val localTerminologyFile = FileOperations.getFileIfExists(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_JSON)
      // find the terminology service
      val localTerminologies = FileOperations.readJsonContent(localTerminologyFile, classOf[LocalTerminology])
      localTerminologies.find(_.id == id) match {
        case Some(foundTerminology) =>
          // check if folders exists
          val terminologyFolder = FileOperations.getFileIfExists(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_FOLDER + File.separator + foundTerminology.name)
          val newTerminologyFolder = new File(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_FOLDER + File.separator + terminology.name)
          if (terminology.name != foundTerminology.name && newTerminologyFolder.exists()) {
            throw AlreadyExists("Local terminology folder already exists.", s"Folder ${terminology.name} already exists.")
          }
          // update the terminology service json
          val newLocalTerminology = localTerminologies.map {
            case t if t.id == id => terminology
            case t => t
          }
          val writer = new FileWriter(localTerminologyFile)
          try {
            writer.write(writePretty(newLocalTerminology))
          } finally {
            writer.close()
          }
          //update folder name if changed
          if (terminology.name != foundTerminology.name) {
            terminologyFolder.renameTo(newTerminologyFolder)
          }
          this.updateConceptMapAndCodeSystemFiles(terminology) // create concept maps
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
      val localTerminologies = FileOperations.readJsonContent(localTerminologyFile, classOf[LocalTerminology])
      localTerminologies.find(_.id == id) match {
        case Some(foundTerminology) =>
          // check if folders exists
          val terminologyFolder = FileOperations.getFileIfExists(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_FOLDER + File.separator + foundTerminology.name)
          // delete the terminology from json
          val newLocalTerminology = localTerminologies.filterNot(_.id == id)
          val writer = new FileWriter(localTerminologyFile)
          try {
            writer.write(writePretty(newLocalTerminology))
          } finally {
            writer.close()
          }
          //delete the terminology folder
          FileUtils.deleteDirectory(terminologyFolder)
        case None => throw ResourceNotFound("Local terminology not found.", s"Local terminology with id $id not found.")
      }
    }
  }

  /**
   * Create/delete concept map and code system files in the terminology folder according to the new terminology
   * @param terminology LocalTerminologyService
   * @return
   */
  private def updateConceptMapAndCodeSystemFiles(terminology: LocalTerminology): Future[Unit] = {
    Future {
      val localTerminologyFolder = FileOperations.getFileIfExists(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_FOLDER + File.separator + terminology.name)
      val currentFiles = localTerminologyFolder.listFiles().filter(_.isFile).map(_.getName)
      val newFiles = terminology.conceptMaps.map(_.name) ++ terminology.codeSystems.map(_.name)
      val filesToDelete = currentFiles.diff(newFiles)
      val filesToAdd = newFiles.diff(currentFiles)
      filesToDelete.foreach { file =>
        val f = new File(localTerminologyFolder + File.separator + file)
        f.delete()
      }
      filesToAdd.foreach { file =>
        val f = new File(localTerminologyFolder + File.separator + file)
        f.createNewFile()
      }
    }
  }
}

object LocalTerminologyFolderRepository {
  val TERMINOLOGY_FOLDER = "terminology-services"
  val TERMINOLOGY_JSON = TERMINOLOGY_FOLDER + File.separator + "terminology-services.json"
}
