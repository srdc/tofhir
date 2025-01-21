package io.tofhir.server.repository.mappingContext

import akka.stream.scaladsl.{FileIO, Source}
import akka.util.ByteString
import io.onfhir.api.util.IOUtil
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.common.model.{AlreadyExists, ResourceNotFound}
import io.tofhir.server.model._
import io.tofhir.server.model.csv.CsvHeader
import io.tofhir.server.repository.project.IProjectRepository
import io.tofhir.server.util.CsvUtil

import java.io.File
import scala.collection.mutable
import scala.concurrent.Future


/**
 * Folder/Directory based mapping context repository implementation.
 *
 * @param mappingContextRepositoryFolderPath root folder path to the mapping context repository
 */
class MappingContextFolderRepository(mappingContextRepositoryFolderPath: String, projectRepository: IProjectRepository) extends IMappingContextRepository {
  // project id -> mapping context id
  private val mappingContextDefinitions: mutable.Map[String, Seq[String]] = mutable.Map.empty[String, Seq[String]]
  // Initialize the map for the first time
  initMap(mappingContextRepositoryFolderPath)

  /**
   * Retrieve the metadata of all mapping context ids
   *
   * @return
   */
  override def getAllMappingContext(projectId: String): Future[Seq[String]] = {
    Future {
      mappingContextDefinitions.getOrElse(projectId, Seq.empty[String])
    }
  }

  /**
   * Save the mapping context to the repository.
   * We only store id of a mapping context in the project metadata json file.
   *
   * @param projectId        project id the mapping context belongs to
   * @param mappingContextId mapping context id to save
   * @return
   */
  override def createMappingContext(projectId: String, mappingContextId: String): Future[String] = {
    if (mappingContextExists(projectId, mappingContextId)) {
      throw AlreadyExists("Fhir mapping already exists.", s"A mapping context definition with id $mappingContextId already exists in the mapping context repository at ${FileUtils.getPath(mappingContextRepositoryFolderPath).toAbsolutePath.toString}")
    }
    // Write to the repository as a new file
    getFileForMappingContext(projectId, mappingContextId).flatMap(newFile => {
      newFile.createNewFile()
      // Add to the project metadata json file
      projectRepository.addMappingContext(projectId, mappingContextId) map { _ =>
        // Add to the in-memory map
        mappingContextDefinitions(projectId) = mappingContextDefinitions.getOrElseUpdate(projectId, Seq.empty) :+ mappingContextId
        mappingContextId
      }
    })
  }


  /**
   * Delete the mapping context from the repository
   *
   * @param projectId        project id the mapping context belongs to
   * @param mappingContextId mapping context id
   * @return
   */
  override def deleteMappingContext(projectId: String, mappingContextId: String): Future[Unit] = {
    if (!mappingContextExists(projectId, mappingContextId)) {
      throw ResourceNotFound("Mapping context does not exists.", s"A mapping context with id $mappingContextId does not exists in the mapping context repository at ${FileUtils.getPath(mappingContextRepositoryFolderPath).toAbsolutePath.toString}")
    }

    // delete the mapping context from the repository
    getFileForMappingContext(projectId, mappingContextId).flatMap(file => {
      file.delete()
      mappingContextDefinitions(projectId) = mappingContextDefinitions(projectId).filterNot(_ == mappingContextId)
      // update the projects metadata json file
      projectRepository.deleteMappingContext(projectId, Some(mappingContextId))
    })
  }

  /**
   * Deletes all mapping contexts associated with a specific project.
   *
   * @param projectId The unique identifier of the project for which mapping contexts should be deleted.
   */
  override def deleteAllMappingContexts(projectId: String): Unit = {
    Future {
      // delete mapping context definitions for the project
      org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(mappingContextRepositoryFolderPath, projectId).toFile)
      // remove project from the cache
      mappingContextDefinitions.remove(projectId)
    } flatMap { _ =>
      // delete project mapping contexts
      projectRepository.deleteMappingContext(projectId)
    }
  }

  /**
   * Update the mapping context header by its id
   *
   * @param projectId        project id the mapping context belongs to
   * @param mappingContextId mapping context id
   * @param headers          mapping context headers
   * @return
   */
  def updateMappingContextHeader(projectId: String, mappingContextId: String, headers: Seq[CsvHeader]): Future[Unit] = {
    if (!mappingContextExists(projectId, mappingContextId)) {
      throw ResourceNotFound("Mapping context does not exists.", s"A mapping context with id $mappingContextId does not exists in the mapping context repository at ${FileUtils.getPath(mappingContextRepositoryFolderPath).toAbsolutePath.toString}")
    }
    // Write headers to the first row in the related file in the repository
    getFileForMappingContext(projectId, mappingContextId).map(file => {
      CsvUtil.writeCsvHeaders(file, headers)
    })
  }

  /**
   * Save the mapping context content to the repository
   *
   * @param projectId        project id the mapping context belongs to
   * @param mappingContextId mapping context id
   * @param content          mapping context content to save
   * @return
   */
  override def saveMappingContextContent(projectId: String, mappingContextId: String, content: Source[ByteString, Any], pageNumber: Int, pageSize: Int): Future[Long] = {
    if (!mappingContextExists(projectId, mappingContextId)) {
      throw ResourceNotFound("Mapping context does not exists.", s"A mapping context with id $mappingContextId does not exists in the mapping context repository at ${FileUtils.getPath(mappingContextRepositoryFolderPath).toAbsolutePath.toString}")
    }
    // Write content to the related file in the repository
    getFileForMappingContext(projectId, mappingContextId).flatMap(file => {
      CsvUtil.writeCsvAndReturnRowNumber(file, content, pageNumber, pageSize)
    })
  }

  /**
   * Get the mapping context content by its id
   *
   * @param projectId        project id the mapping context belongs to
   * @param mappingContextId mapping context id
   * @return
   */
  override def getMappingContextContent(projectId: String, mappingContextId: String, pageNumber: Int, pageSize: Int): Future[(Source[ByteString, Any], Long)] = {
    if (!mappingContextExists(projectId, mappingContextId)) {
      throw ResourceNotFound("Mapping context does not exists.", s"A mapping context with id $mappingContextId does not exists in the mapping context repository at ${FileUtils.getPath(mappingContextRepositoryFolderPath).toAbsolutePath.toString}")
    }
    // Read content from the related file in the repository
    getFileForMappingContext(projectId, mappingContextId).flatMap(file => {
      CsvUtil.getPaginatedCsvContent(file, pageNumber, pageSize)
    })
  }

  /**
   * Upload the mapping context content to the repository
   *
   * @param projectId        project id the mapping context belongs to
   * @param mappingContextId mapping context id
   * @param content          mapping context content to save
   * @return
   */
  override def uploadMappingContext(projectId: String, mappingContextId: String, content: Source[ByteString, Any]): Future[Unit] = {
    if (!mappingContextExists(projectId, mappingContextId)) {
      throw ResourceNotFound("Mapping context does not exists.", s"A mapping context with id $mappingContextId does not exists in the mapping context repository at ${FileUtils.getPath(mappingContextRepositoryFolderPath).toAbsolutePath.toString}")
    }
    // Write content to the related file in the repository
    getFileForMappingContext(projectId, mappingContextId).map(file => {
      CsvUtil.saveFileContent(file, content)
    })
  }

  /**
   * Download the mapping context content by its id
   *
   * @param projectId        project id the mapping context belongs to
   * @param mappingContextId mapping context id
   * @return
   */
  override def downloadMappingContext(projectId: String, mappingContextId: String): Future[Source[ByteString, Any]] = {
    if (!mappingContextExists(projectId, mappingContextId)) {
      throw ResourceNotFound("Mapping context does not exists.", s"A mapping context with id $mappingContextId does not exists in the mapping context repository at ${FileUtils.getPath(mappingContextRepositoryFolderPath).toAbsolutePath.toString}")
    }
    // Read content from the related file in the repository
    getFileForMappingContext(projectId, mappingContextId).map(file => {
      FileIO.fromPath(file.toPath)
    })
  }

  /**
   * Checks if the mapping context exists in the repository
   *
   * @param projectId        project id the mapping context belongs to
   * @param mappingContextId mapping context id
   * @return
   */
  private def mappingContextExists(projectId: String, mappingContextId: String): Boolean = {
    mappingContextDefinitions.get(projectId).exists(_.contains(mappingContextId))
  }

  /**
   * Gets the file for the given mapping context id.
   *
   * @param fhirMappingContextId
   * @return
   */
  private def getFileForMappingContext(projectId: String, fhirMappingContextId: String): Future[File] = {
    val projectFuture: Future[Option[Project]] = projectRepository.getProject(projectId)
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
   *
   * @param mappingContextRepositoryFolderPath path to the mapping context repository
   * @return
   */
  private def initMap(mappingContextRepositoryFolderPath: String): Unit = {
    val folder = FileUtils.getPath(mappingContextRepositoryFolderPath).toFile
    if (!folder.exists()) {
      folder.mkdirs()
    }
    var directories = Seq.empty[File]
    directories = folder.listFiles.filter(_.isDirectory).toSeq
    directories.foreach { projectDirectory =>
      val files = IOUtil.getFilesFromFolder(projectDirectory, recursively = true, ignoreHidden = true, withExtension = None)
      val fileNameList = files.map(_.getName)
      this.mappingContextDefinitions.put(projectDirectory.getName, fileNameList)
    }
  }

  /**
   * Reload the mapping context definitions from the given folder
   *
   * @return
   */
  override def invalidate(): Unit = {
    this.mappingContextDefinitions.clear()
    initMap(mappingContextRepositoryFolderPath)
  }

  /**
   * Retrieve the projects and MappingContextIds within.
   *
   * @return Map of projectId -> Seq[String]
   */
  override def getProjectPairs: Map[String, Seq[String]] = {
    mappingContextDefinitions.toMap
  }
}
