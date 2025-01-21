package io.tofhir.server.repository.mapping

import com.typesafe.scalalogging.Logger
import io.onfhir.api.util.IOUtil
import io.onfhir.definitions.common.model.Json4sSupport.formats
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.model.FhirMapping
import io.tofhir.engine.util.FileUtils
import io.tofhir.engine.util.FileUtils.FileExtensions
import io.tofhir.server.common.model.{AlreadyExists, ResourceNotFound}
import io.tofhir.server.repository.project.IProjectRepository
import io.tofhir.server.util.FileOperations
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.Serialization.writePretty

import java.io.{File, FileWriter}
import java.nio.charset.StandardCharsets
import scala.collection.mutable
import scala.concurrent.Future
import scala.io.Source

/**
 * Folder/Directory based mapping repository implementation. This implementation manages [[FhirMapping]]s per project.
 * It also extends the engine's folder-based mapping repository implementation to be able to use the same business logic to load mappings from folders.
 *
 * @param mappingRepositoryFolderPath root folder path to the mapping repository
 * @param projectRepository           project repository to update corresponding projects based on updates on the mappings
 */
class ProjectMappingFolderRepository(mappingRepositoryFolderPath: String, projectRepository: IProjectRepository) extends IMappingRepository {

  private val logger: Logger = Logger(this.getClass)

  // project id -> mapping id -> mapping
  private val mappingDefinitions: mutable.Map[String, mutable.Map[String, FhirMapping]] = mutable.Map.empty[String, mutable.Map[String, FhirMapping]]
  // Initialize the map for the first time
  initMap(mappingRepositoryFolderPath)

  /**
   * Retrieve all mappings of a given project
   *
   * @param projectId project id the jobs belong to
   * @return
   */
  override def getAllMappings(projectId: String): Future[Seq[FhirMapping]] = {
    Future {
      mappingDefinitions.get(projectId)
        .map(_.values.toSeq) // If such a project exists, return the mappings as a sequence
        .getOrElse(Seq.empty[FhirMapping]) // Else, return an empty list
    }
  }

  /**
   * Save the mapping to the repository.
   *
   * @param projectId subfolder to save the mapping in
   * @param mapping   mapping to save
   * @return the saved FhirMapping
   */
  override def saveMapping(projectId: String, mapping: FhirMapping): Future[FhirMapping] = {
    // validate that mapping id is unique
    mappingDefinitions.get(projectId).flatMap(_.get(mapping.id)).foreach { _ =>
      throw AlreadyExists("Fhir mapping already exists.", s"A mapping definition with id ${mapping.id} already exists in the mapping repository at ${FileUtils.getPath(mappingRepositoryFolderPath).toAbsolutePath.toString}")
    }
    // validate that mapping url is unique
    mappingDefinitions.get(projectId).foreach { mappings =>
      if (mappings.values.exists(_.url.contentEquals(mapping.url))) {
        throw AlreadyExists("Fhir mapping already exists.", s"A mapping definition with url ${mapping.url} already exists in the mapping repository at ${FileUtils.getPath(mappingRepositoryFolderPath).toAbsolutePath.toString}")
      }
    }

    // Write to the repository as a new file
    getFileForMapping(projectId, mapping.id).flatMap(newFile => {
      val fw = new FileWriter(newFile)
      fw.write(writePretty(mapping))
      fw.close()

      // Add to the project metadata json file
      projectRepository.addMapping(projectId, mapping) map { _ =>
        // Add to the in-memory map
        mappingDefinitions.getOrElseUpdate(projectId, mutable.Map.empty).put(mapping.id, mapping)
        mapping
      }
    })
  }

  /**
   * Get the mapping by its id
   *
   * @param projectId project id the mapping belongs to
   * @param mappingId mapping id
   * @return
   */
  override def getMapping(projectId: String, mappingId: String): Future[Option[FhirMapping]] = {
    Future {
      mappingDefinitions(projectId).get(mappingId)
    }
  }

  /**
   * Update the mapping in the repository
   *
   * @param projectId project id the mapping belongs to
   * @param mappingId mapping id
   * @param mapping   mapping to save
   * @return
   * @throws AlreadyExists if a mapping with the same URL already exists
   */
  override def updateMapping(projectId: String, mappingId: String, mapping: FhirMapping): Future[FhirMapping] = {
    if (!mappingDefinitions.get(projectId).exists(_.contains(mappingId))) {
      throw ResourceNotFound("Mapping does not exists.", s"A mapping with id $mappingId does not exists in the mapping repository at ${FileUtils.getPath(mappingRepositoryFolderPath).toAbsolutePath.toString}")
    }
    // validate that mapping url is unique
    mappingDefinitions.get(projectId).foreach { mappings =>
      if (mappings.exists { case (id, definition) => !id.contentEquals(mappingId) && definition.url.contentEquals(mapping.url) }) {
        throw AlreadyExists("Fhir mapping already exists.", s"A mapping definition with url ${mapping.url} already exists in the mapping repository at ${FileUtils.getPath(mappingRepositoryFolderPath).toAbsolutePath.toString}")
      }
    }

    // update the mapping in the repository
    getFileForMapping(projectId, mapping.id).flatMap(file => {
      val fw = new FileWriter(file)
      fw.write(writePretty(mapping))
      fw.close()
      // update the mapping in the in-memory map
      mappingDefinitions(projectId).put(mappingId, mapping)
      // update the projects metadata json file
      projectRepository.updateMapping(projectId, mapping) map { _ =>
        mapping
      }
    })
  }

  /**
   * Delete the mapping from the repository
   *
   * @param projectId project id the mapping belongs to
   * @param mappingId mapping id
   * @return
   */
  override def deleteMapping(projectId: String, mappingId: String): Future[Unit] = {
    if (!mappingDefinitions.get(projectId).exists(_.contains(mappingId))) {
      throw ResourceNotFound("Mapping does not exists.", s"A mapping with id $mappingId does not exists in the mapping repository at ${FileUtils.getPath(mappingRepositoryFolderPath).toAbsolutePath.toString}")
    }

    // delete the mapping from the repository
    getFileForMapping(projectId, mappingDefinitions(projectId)(mappingId).id).flatMap(file => {
      file.delete()
      mappingDefinitions(projectId).remove(mappingId)
      // delete the mapping from projects json file
      projectRepository.deleteMapping(projectId, Some(mappingId))
    })
  }

  /**
   * Deletes all mappings associated with a specific project.
   *
   * @param projectId The unique identifier of the project for which mappings should be deleted.
   */
  override def deleteAllMappings(projectId: String): Future[Unit] = {
    Future {
      // delete mapping definitions for the project
      org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(mappingRepositoryFolderPath, projectId).toFile)
      // remove project from the cache
      mappingDefinitions.remove(projectId)
    } flatMap { _ =>
      // delete project mappings
      projectRepository.deleteMapping(projectId)
    }
  }

  /**
   * Retrieves the identifiers of mappings referencing the given schema in their definitions.
   *
   * @param projectId identifier of project whose mappings will be checked
   * @param schemaUrl the url of schema
   * @return the identifiers of mappings referencing the given schema in their definitions
   */
  override def getMappingsReferencingSchema(projectId: String, schemaUrl: String): Future[Seq[String]] = {
    Future {
      mappingDefinitions.getOrElse(projectId, Map.empty) // handle the case where project has no mappings by returning an empty Map
        .values.toSeq
        .filter(mapping => mapping.source.exists(source => source.url.contentEquals(schemaUrl)))
        .map(mapping => mapping.id)
    }
  }

  /**
   * Gets the file for the given mapping definition.
   *
   * @param fhirMapping
   * @return
   */
  private def getFileForMapping(projectId: String, fhirMappingId: String): Future[File] = {
    projectRepository.getProject(projectId) map { project =>
      if (project.isEmpty) throw new IllegalStateException(s"This should not be possible. ProjectId: $projectId does not exist in the project folder repository.")
      FileOperations.getFileForEntityWithinProject(mappingRepositoryFolderPath, project.get.id, fhirMappingId)
    }
  }

  /**
   * Initializes the mapping definitions from the repository
   *
   * @param mappingRepositoryFolderPath path to the mapping repository
   * @return
   */
  private def initMap(mappingRepositoryFolderPath: String): Unit = {
    val mappingRepositoryFolder = FileUtils.getPath(mappingRepositoryFolderPath).toFile
    if (!mappingRepositoryFolder.exists()) {
      mappingRepositoryFolder.mkdirs()
    }
    var projectDirectories = Seq.empty[File]
    projectDirectories = mappingRepositoryFolder.listFiles.filter(_.isDirectory).toSeq
    projectDirectories.foreach { projectDirectory =>
      // mapping-id -> FhirMapping
      val fhirMappingMap: mutable.Map[String, FhirMapping] = mutable.Map.empty
      val files = IOUtil.getFilesFromFolder(projectDirectory, recursively = true, ignoreHidden = true, withExtension = Some(FileExtensions.JSON.toString))
      files.foreach { file =>
        val source = Source.fromFile(file, StandardCharsets.UTF_8.name()) // read the JSON file
        val fileContent = try source.mkString finally source.close()
        try { // Try to parse the file content as FhirMapping
          val fhirMapping = JsonMethods.parse(fileContent).extract[FhirMapping]
          // discard if the mapping id and file name not match
          if (FileOperations.checkFileNameMatchesEntityId(fhirMapping.id, file, "mapping")) {
            fhirMappingMap.put(fhirMapping.id, fhirMapping)
          } // else case is logged within FileOperations.checkFileNameMatchesEntityId
        } catch {
          case e: Throwable =>
            logger.error(s"Failed to parse mapping definition at ${file.getPath}", e)
            System.exit(1)
        }
      }
      if (fhirMappingMap.isEmpty) {
        // No processable schema files under projectDirectory
        logger.warn(s"There are no processable mapping files under ${projectDirectory.getAbsolutePath}. Skipping ${projectDirectory.getName}.")
      } else {
        this.mappingDefinitions.put(projectDirectory.getName, fhirMappingMap)
      }
    }
  }

  /**
   * Returns the Fhir mapping definition by given url
   *
   * @param mappingUrl Fhir mapping url
   * @return
   */
  override def getFhirMappingByUrl(mappingUrl: String): FhirMapping = {
    mappingDefinitions.values
      .flatMap(_.values) // Flatten all the mappings managed for all projects
      .find(_.url.contentEquals(mappingUrl))
      .get
  }

  /**
   * Invalidate the internal cache and refresh the cache with the FhirMappings directly from their source
   */
  override def invalidate(): Unit = {
    this.mappingDefinitions.clear()
    initMap(mappingRepositoryFolderPath)
  }

  /**
   * Retrieve the projects and FhirMappings within.
   *
   * @return Map of projectId -> Seq[FhirMapping]
   */
  override def getProjectPairs: Map[String, Seq[FhirMapping]] = {
    mappingDefinitions.map { case (projectId, mappingPairs) =>
      projectId -> mappingPairs.values.toSeq
    }.toMap
  }

}
