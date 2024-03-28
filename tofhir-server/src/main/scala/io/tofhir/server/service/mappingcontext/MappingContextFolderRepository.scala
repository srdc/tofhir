package io.tofhir.server.service.mappingcontext

import akka.NotUsed
import akka.stream.scaladsl.{Concat, FileIO, Framing, Keep, Sink, Source}
import akka.util.ByteString
import io.onfhir.api.util.IOUtil
import io.onfhir.util.DateTimeUtil.logger
import io.tofhir.engine.Execution.actorSystem
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.model.FhirMappingException
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.common.model.{AlreadyExists, ResourceNotFound}
import io.tofhir.server.model._
import io.tofhir.server.service.project.ProjectFolderRepository
import io.tofhir.server.util.{CsvUtil, FileOperations}

import java.io.File
import java.nio.file.StandardOpenOption
import scala.collection.mutable
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}


/**
 * Folder/Directory based mapping context repository implementation.
 *
 * @param mappingContextRepositoryFolderPath root folder path to the mapping context repository
 */
class MappingContextFolderRepository(mappingContextRepositoryFolderPath: String, projectFolderRepository: ProjectFolderRepository) extends IMappingContextRepository {
  // project id -> mapping context id
  private val mappingContextDefinitions: mutable.Map[String, Seq[String]] = initMap(mappingContextRepositoryFolderPath)

  /**
   * Returns the mapping context cached in memory
   *
   * @return
   */
  def getCachedMappingContexts(): mutable.Map[String, Seq[String]] = {
    mappingContextDefinitions
  }

  /**
   * Retrieve the metadata of all mapping context ids
   *
   * @return
   */
  override def getAllMappingContext(projectId: String): Future[Seq[String]] = {
    Future {
      if (mappingContextDefinitions.contains(projectId)) {
        mappingContextDefinitions(projectId)
      } else {
        Seq.empty
      }
    }
  }

  /**
   * Save the mapping context to the repository.
   * We only store id of a mapping context in the project metadata json file.
   *
   * @param projectId project id the mapping context belongs to
   * @param id        mapping context id to save
   * @return
   */
  override def createMappingContext(projectId: String, id: String): Future[String] = {
    if (mappingContextExists(projectId, id)) {
      throw AlreadyExists("Fhir mapping already exists.", s"A mapping context definition with id ${id} already exists in the mapping context repository at ${FileUtils.getPath(mappingContextRepositoryFolderPath).toAbsolutePath.toString}")
    }
    // Write to the repository as a new file
    getFileForMappingContext(projectId, id).map(newFile => {
      newFile.createNewFile()
      // Add to the project metadata json file
      projectFolderRepository.addMappingContext(projectId, id)
      // Add to the in-memory map
      mappingContextDefinitions(projectId) = mappingContextDefinitions.getOrElseUpdate(projectId, Seq.empty) :+ id
      id
    })
  }


  /**
   * Delete the mapping context from the repository
   *
   * @param projectId project id the mapping context belongs to
   * @param id        mapping context id
   * @return
   */
  override def deleteMappingContext(projectId: String, id: String): Future[Unit] = {
    if (!mappingContextExists(projectId, id)) {
      throw ResourceNotFound("Mapping context does not exists.", s"A mapping context with id $id does not exists in the mapping context repository at ${FileUtils.getPath(mappingContextRepositoryFolderPath).toAbsolutePath.toString}")
    }

    // delete the mapping context from the repository
    getFileForMappingContext(projectId, id).map(file => {
      file.delete()
      mappingContextDefinitions(projectId) = mappingContextDefinitions(projectId).filterNot(_ == id)
      // update the projects metadata json file
      projectFolderRepository.deleteMappingContext(projectId, Some(id))
    })
  }

  /**
   * Deletes all mapping contexts associated with a specific project.
   *
   * @param projectId The unique identifier of the project for which mapping contexts should be deleted.
   */
  override def deleteProjectMappingContexts(projectId: String): Unit = {
    // delete mapping context definitions for the project
    org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(mappingContextRepositoryFolderPath, projectId).toFile)
    // remove project from the cache
    mappingContextDefinitions.remove(projectId)
    // delete project mapping contexts
    projectFolderRepository.deleteMappingContext(projectId)
  }

  /**
   * Save the mapping context content to the repository
   *
   * @param projectId project id the mapping context belongs to
   * @param id        mapping context id
   * @param content   mapping context content to save
   * @return
   */
  override def saveMappingContextContent(projectId: String, id: String, content: Source[ByteString, Any], pageNumber: Int, pageSize: Int): Future[Long] = {
    if (!mappingContextExists(projectId, id)) {
      throw ResourceNotFound("Mapping context does not exists.", s"A mapping context with id $id does not exists in the mapping context repository at ${FileUtils.getPath(mappingContextRepositoryFolderPath).toAbsolutePath.toString}")
    }
    // Write content to the related file in the repository
    getFileForMappingContext(projectId, id).flatMap(file => {
      CsvUtil.writeCsvAndReturnRowNumber(file, content, pageNumber, pageSize)
    })
  }

  /**
   * Get the mapping context content by its id
   *
   * @param projectId project id the mapping context belongs to
   * @param id        mapping context id
   * @return
   */
  override def getMappingContextContent(projectId: String, id: String, pageNumber: Int, pageSize: Int): Future[(Source[ByteString, Any], Long)] = {
    if (!mappingContextExists(projectId, id)) {
      throw ResourceNotFound("Mapping context does not exists.", s"A mapping context with id $id does not exists in the mapping context repository at ${FileUtils.getPath(mappingContextRepositoryFolderPath).toAbsolutePath.toString}")
    }
    // Read content from the related file in the repository
    getFileForMappingContext(projectId, id).flatMap(file => {
      CsvUtil.getPaginatedCsvContent(file, pageNumber, pageSize)
    })
  }

  /**
   * Checks if the mapping context exists in the repository
   *
   * @param projectId project id the mapping context belongs to
   * @param id mapping context id
   * @return
   */
  private def mappingContextExists(projectId: String, id: String): Boolean = {
    mappingContextDefinitions.contains(projectId) && mappingContextDefinitions(projectId).contains(id)
  }

  /**
   * Gets the file for the given mapping context id.
   *
   * @param fhirMappingContextId
   * @return
   */
  private def getFileForMappingContext(projectId: String, fhirMappingContextId: String): Future[File] = {
    val projectFuture: Future[Option[Project]] = projectFolderRepository.getProject(projectId)
    projectFuture.map(project => {
      val file: File = FileUtils.getPath(mappingContextRepositoryFolderPath, project.get.id, fhirMappingContextId).toFile
      // If the project folder does not exist, create it
      if (!file.getParentFile.exists()) {
        file.getParentFile.mkdir()
      }
      file
    })
  }

  /**
   * Initializes the mapping context map from the repository
   * Map of (project id -> mapping context list)
   * @param mappingContextRepositoryFolderPath path to the mapping context repository
   * @return
   */
  private def initMap(mappingContextRepositoryFolderPath: String): mutable.Map[String, Seq[String]] = {
    val map = mutable.Map.empty[String, Seq[String]]
    val folder = FileUtils.getPath(mappingContextRepositoryFolderPath).toFile
    if (!folder.exists()) {
      folder.mkdirs()
    }
    var directories = Seq.empty[File]
    directories = folder.listFiles.filter(_.isDirectory).toSeq
    directories.foreach { projectDirectory =>
      val files = IOUtil.getFilesFromFolder(projectDirectory, withExtension = None, recursively = Some(true))
      val fileNameList = files.map(_.getName)
      map.put(projectDirectory.getName, fileNameList)
    }
    map
  }

}
