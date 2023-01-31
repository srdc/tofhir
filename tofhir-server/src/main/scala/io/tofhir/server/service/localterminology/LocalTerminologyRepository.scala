package io.tofhir.server.service.localterminology

import io.onfhir.util.JsonFormatter._
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.server.model.{AlreadyExists, BadRequest, InternalError, LocalTerminology, ResourceNotFound}
import io.tofhir.server.service.localterminology.ILocalTerminologyRepository.{TERMINOLOGY_FOLDER, TERMINOLOGY_JSON}
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
class LocalTerminologyRepository(localTerminologyRepositoryRoot: String) extends ILocalTerminologyRepository {

  /**
   * Retrieve the metadata of all LocalTerminology
   * @return Seq[LocalTerminology]
   */
  override def getAllLocalTerminologyMetadata: Future[Seq[LocalTerminology]] = {
    Future {
      val localTerminologyFile = new File(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_JSON)
      if (localTerminologyFile.exists()) {
        val localTerminology = FileOperations.readJsonContent(localTerminologyFile, classOf[LocalTerminology])
        localTerminology
      } else {
        throw InternalError("Local terminology json file not found.", s"$TERMINOLOGY_JSON file should exists in the root folder.")
      }
    }
  }

  /**
   * Create a new LocalTerminology
   * @return LocalTerminology
   */
  override def createTerminologyService(terminology: LocalTerminology): Future[LocalTerminology] = {
    Future {
      val localTerminologyFile = new File(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_JSON)
      if (localTerminologyFile.exists()) {
        // check if folder exists
        val terminologyFolder = new File(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_FOLDER + File.separator + terminology.folderPath)
        if (terminologyFolder.exists()) {
          throw AlreadyExists("Local terminology folder already exists.", s"Folder ${terminology.folderPath} already exists.")
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
        terminology
      } else {
        throw InternalError("Local terminology json file not found.", s"$TERMINOLOGY_JSON file should exists in the root folder.")
      }
    }
  }

  /**
   * Retrieve a LocalTerminologyService
   * @param id id of the LocalTerminologyService
   * @return LocalTerminologyService
   */
  override def retrieveTerminology(id: String): Future[LocalTerminology] = {
    Future {
      val localTerminologyFile = new File(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_JSON)
      if (localTerminologyFile.exists()) {
        val localTerminologies = FileOperations.readJsonContent(localTerminologyFile, classOf[LocalTerminology])
        localTerminologies.find(_.id == id) match {
          case Some(terminology) => terminology
          case None => throw ResourceNotFound("Local terminology not found.", s"Local terminology with id $id not found.")
        }
      } else {
        throw InternalError("Local terminology json file not found.", s"$TERMINOLOGY_JSON file should exists in the root folder.")
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
      val localTerminologyFile = new File(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_JSON)
      if (localTerminologyFile.exists()) {
        // find the terminology service
        val localTerminologies = FileOperations.readJsonContent(localTerminologyFile, classOf[LocalTerminology])
        localTerminologies.find(_.id == id) match {
          case Some(foundTerminology) =>
            // check if folders exists
            val terminologyFolder = new File(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_FOLDER + File.separator + foundTerminology.folderPath)
            if (!terminologyFolder.exists()) {
              throw BadRequest("Local terminology folder does not exist.", s"Folder ${foundTerminology.folderPath} does not exist.")
            }
            val newTerminologyFolder = new File(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_FOLDER + File.separator + terminology.folderPath)
            if (terminology.folderPath != foundTerminology.folderPath && newTerminologyFolder.exists()) {
              throw AlreadyExists("Local terminology folder already exists.", s"Folder ${terminology.folderPath} already exists.")
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
            if (terminology.folderPath != foundTerminology.folderPath) {
              terminologyFolder.renameTo(newTerminologyFolder)
            }
            terminology
          case None => throw ResourceNotFound("Local terminology not found.", s"Local terminology with id $id not found.")
        }
      } else {
        throw InternalError("Local terminology json file not found.", s"$TERMINOLOGY_JSON file should exists in the root folder.")
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
      val localTerminologyFile = new File(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_JSON)
      if (localTerminologyFile.exists()) {
        // find the terminology service
        val localTerminologies = FileOperations.readJsonContent(localTerminologyFile, classOf[LocalTerminology])
        localTerminologies.find(_.id == id) match {
          case Some(foundTerminology) =>
            // check if folders exists
            val terminologyFolder = new File(localTerminologyRepositoryRoot + File.separator + TERMINOLOGY_FOLDER + File.separator + foundTerminology.folderPath)
            if (!terminologyFolder.exists()) {
              throw BadRequest("Local terminology folder does not exist.", s"Folder ${foundTerminology.folderPath} does not exist.")
            }
            // delete the terminology from json
            val newLocalTerminology = localTerminologies.filterNot(_.id == id)
            val writer = new FileWriter(localTerminologyFile)
            try {
              writer.write(writePretty(newLocalTerminology))
            } finally {
              writer.close()
            }
            //delete folder
            FileUtils.deleteDirectory(terminologyFolder)
          case None => throw ResourceNotFound("Local terminology not found.", s"Local terminology with id $id not found.")
        }
      } else {
        throw InternalError("Local terminology json file not found.", s"$TERMINOLOGY_JSON file should exists in the root folder.")
      }
    }
  }
}


