package io.tofhir.server.service.mapping

import io.onfhir.api.util.IOUtil
import io.onfhir.util.JsonFormatter._
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.config.ToFhirEngineConfig
import io.tofhir.engine.model.{FhirMapping, FhirMappingException}
import io.tofhir.engine.util.FileUtils
import io.tofhir.engine.util.FileUtils.FileExtensions
import io.tofhir.server.model.{AlreadyExists, BadRequest, Project, ResourceNotFound}
import io.tofhir.server.service.project.ProjectFolderRepository
import org.json4s._
import org.json4s.jackson.Serialization.writePretty

import java.io.{File, FileWriter}
import java.nio.charset.StandardCharsets
import scala.collection.mutable
import scala.concurrent.Future
import scala.io.Source

/**
 * Folder/Directory based mapping repository implementation.
 *
 * @param toFhirEngineConfig configuration for the ToFhir engine, including the mapping repository folder path
 */
class MappingFolderRepository(toFhirEngineConfig: ToFhirEngineConfig, projectFolderRepository: ProjectFolderRepository) extends IMappingRepository {
  // project id -> mapping id -> mapping
  private val mappingDefinitions: mutable.Map[String, mutable.Map[String, FhirMapping]] = initMap(toFhirEngineConfig.mappingRepositoryFolderPath)

  /**
   * Returns the mappings managed by this repository
   * @return
   */
  def getCachedMappings(): mutable.Map[String, mutable.Map[String, FhirMapping]] = {
    mappingDefinitions
  }

  /**
   * Retrieve the metadata of all MappingFile (only url, type and name fields are populated)
   *
   * @return
   */
  override def getAllMappings(projectId: String): Future[Seq[FhirMapping]] = {
    Future {
      if (mappingDefinitions.contains(projectId)) {
        mappingDefinitions(projectId).values.toSeq
      } else {
        Seq.empty
      }
    }
  }

  /**
   * Save the mapping to the repository.
   *
   * @param projectId  subfolder to save the mapping in
   * @param mapping mapping to save
   * @return
   */
  override def createMapping(projectId: String, mapping: FhirMapping): Future[FhirMapping] = {
    Future {
      if (mappingDefinitions.contains(projectId) && mappingDefinitions(projectId).contains(mapping.id)) {
        throw AlreadyExists("Fhir mapping already exists.", s"A mapping definition with id ${mapping.id} already exists in the mapping repository at ${FileUtils.getPath(toFhirEngineConfig.mappingRepositoryFolderPath).toAbsolutePath.toString}")
      }
      // add relative paths to the mapping
      val pathFixedMapping = addRelativePaths(projectId, mapping)
      // Write to the repository as a new file
      getFileForMapping(projectId, pathFixedMapping).map(newFile => {
        val fw = new FileWriter(newFile)
        fw.write(writePretty(pathFixedMapping))
        fw.close()
      })
      // Add to the project metadata json file
      projectFolderRepository.addMapping(projectId, pathFixedMapping)
      // Add to the in-memory map without the relative paths
      mappingDefinitions.getOrElseUpdate(projectId, mutable.Map.empty).put(mapping.id, mapping)
      mapping
    }
  }

  /**
   * Get the mapping by its id
   *
   * @param projectId project id the mapping belongs to
   * @param id mapping id
   * @return
   */
  override def getMapping(projectId: String, id: String): Future[Option[FhirMapping]] = {
    Future {
      mappingDefinitions(projectId).get(id)
    }
  }

  /**
   * Update the mapping in the repository
   *
   * @param projectId project id the mapping belongs to
   * @param id        mapping id
   * @param mapping   mapping to save
   * @return
   */
  override def putMapping(projectId: String, id: String, mapping: FhirMapping): Future[FhirMapping] = {
    Future {
      // cross check ids
      if (!id.equals(mapping.id)) {
        throw BadRequest("Mapping definition is not valid.", s"Identifier of the mapping definition: ${mapping.id} does not match with explicit id: $id")
      }
      if (!mappingDefinitions.contains(projectId) || !mappingDefinitions(projectId).contains(id)) {
        throw ResourceNotFound("Mapping does not exists.", s"A mapping with id $id does not exists in the mapping repository at ${FileUtils.getPath(toFhirEngineConfig.mappingRepositoryFolderPath).toAbsolutePath.toString}")
      }
      // add relative paths to the mapping
      val pathFixedMapping = addRelativePaths(projectId, mapping)
      // update the mapping in the repository
      getFileForMapping(projectId, pathFixedMapping).map(file => {
        val fw = new FileWriter(file)
        fw.write(writePretty(pathFixedMapping))
        fw.close()
      })
      // update the mapping in the in-memory map without the relative paths
      mappingDefinitions(projectId).put(id, mapping)
      // update the projects metadata json file
      projectFolderRepository.updateMapping(projectId, mapping)
      mapping
    }

  }

  /**
   * Delete the mapping from the repository
   *
   * @param projectId project id the mapping belongs to
   * @param id        mapping id
   * @return
   */
  override def deleteMapping(projectId: String, id: String): Future[Unit] = {
    Future {
      if (!mappingDefinitions.contains(projectId) || !mappingDefinitions(projectId).contains(id)) {
        throw ResourceNotFound("Mapping does not exists.", s"A mapping with id $id does not exists in the mapping repository at ${FileUtils.getPath(toFhirEngineConfig.mappingRepositoryFolderPath).toAbsolutePath.toString}")
      }
      // delete the mapping from the repository
      getFileForMapping(projectId, mappingDefinitions(projectId)(id)).map(file => {
        file.delete()
      })
      // delete the mapping from the map
      mappingDefinitions(projectId).remove(id)
      // delete the mapping from projects json file
      projectFolderRepository.deleteMapping(projectId, id)
    }
  }

  /**
   * Gets the file for the given mapping definition.
   *
   * @param fhirMapping
   * @return
   */
  private def getFileForMapping(projectId: String, fhirMapping: FhirMapping): Future[File] = {
    val projectFuture: Future[Option[Project]] = projectFolderRepository.getProject(projectId)
    projectFuture.map(project => {
      val file: File = FileUtils.getPath(toFhirEngineConfig.mappingRepositoryFolderPath, project.get.id, getFileName(fhirMapping.id)).toFile
      // If the project folder does not exist, create it
      if (!file.getParentFile.exists()) {
        file.getParentFile.mkdir()
      }
      file
    })
  }

  /**
   * Constructs the file name for the mapping file given the id and name
   *
   * @param mappingId
   * @return
   */
  private def getFileName(mappingId: String): String = {
    s"$mappingId${FileExtensions.JSON}"
  }

  /**
   * Removes the relative paths for the given mapping context
   * @param mappingContextPath relative path to the mapping context from the mapping
   * @return
   */
  private def removeRelativePathForContext(mappingContextPath: String): Option[String] = {
    mappingContextPath.split("/").lastOption
  }

  /**
   * Adds the relative path for the given mapping context from the mapping
   * @param projectId project id the mapping and mapping context belongs to
   * @param mappingContext mapping context id
   * @return
   */
  private def addRelativePathForContext(projectId: String, mappingContext: String): Option[String] = {
     Some(s"../../${toFhirEngineConfig.mappingContextRepositoryFolderPath}/${projectId}/${mappingContext}");
  }

  /**
   * Adds the relative paths in the mapping
   * @param projectId
   * @param mapping
   * @return
   */
  private def addRelativePaths(projectId: String, mapping: FhirMapping): FhirMapping = {
    mapping.copy(
      context = mapping.context.map(mappingContext => (mappingContext._1 -> mappingContext._2.copy(
        url = addRelativePathForContext(projectId, mappingContext._2.url.get)
      )))
    )
  }

  /**
   * Removes the relative paths in the mapping
   * @param mapping
   * @return
   */
  private def removeRelativePaths(mapping: FhirMapping): FhirMapping = {
    mapping.copy(
      context = mapping.context.map(mappingContext => (mappingContext._1 -> mappingContext._2.copy(
        url = removeRelativePathForContext(mappingContext._2.url.get)
      )))
    )
  }

  /**
   * Initializes the mapping definitions from the repository
   * @param mappingRepositoryFolderPath path to the mapping repository
   * @return
   */
  private def initMap(mappingRepositoryFolderPath: String): mutable.Map[String, mutable.Map[String, FhirMapping]] = {
    val map = mutable.Map.empty[String, mutable.Map[String, FhirMapping]]
    val folder = FileUtils.getPath(mappingRepositoryFolderPath).toFile
    if (!folder.exists()) {
      folder.mkdirs()
    }
    var directories = Seq.empty[File]
    directories = folder.listFiles.filter(_.isDirectory).toSeq
    directories.foreach { projectDirectory =>
      // mapping-id -> FhirMapping
      val fhirMappingMap: mutable.Map[String, FhirMapping] = mutable.Map.empty
      val files = IOUtil.getFilesFromFolder(projectDirectory, withExtension = Some(FileExtensions.JSON.toString), recursively = Some(true))
      files.foreach { file =>
        val source = Source.fromFile(file, StandardCharsets.UTF_8.name()) // read the JSON file
        val fileContent = try source.mkString finally source.close()
        val fhirMapping = fileContent.parseJson.extract[FhirMapping]
        fhirMappingMap.put(fhirMapping.id, removeRelativePaths(fhirMapping))
      }
      map.put(projectDirectory.getName, fhirMappingMap)
    }
    map
  }

}
