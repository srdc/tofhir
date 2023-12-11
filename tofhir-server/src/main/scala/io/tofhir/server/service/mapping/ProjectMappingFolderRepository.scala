package io.tofhir.server.service.mapping

import com.fasterxml.jackson.core.JsonParseException
import com.typesafe.scalalogging.Logger
import io.onfhir.util.JsonFormatter._
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.model.FhirMapping
import io.tofhir.engine.util.FileUtils
import io.tofhir.engine.util.FileUtils.FileExtensions
import io.tofhir.server.model.{AlreadyExists, BadRequest, Project, ResourceNotFound}
import io.tofhir.server.service.project.ProjectFolderRepository
import org.json4s.jackson.Serialization.writePretty

import java.io.{File, FileWriter}
import java.nio.charset.StandardCharsets
import io.onfhir.api.util.IOUtil
import io.tofhir.server.util.FileOperations

import scala.collection.mutable
import scala.concurrent.Future
import scala.io.Source

/**
 * Folder/Directory based mapping repository implementation. This implementation manages [[FhirMapping]]s per project.
 * It also extends the engine's folder-based mapping repository implementation to be able to use the same business logic to load mappings from folders.
 *
 * @param mappingRepositoryFolderPath root folder path to the mapping repository
 * @param projectFolderRepository     project repository to update corresponding projects based on updates on the mappings
 */
class ProjectMappingFolderRepository(mappingRepositoryFolderPath: String, projectFolderRepository: ProjectFolderRepository) extends IMappingRepository {

  private val logger: Logger = Logger(this.getClass)

  // project id -> mapping id -> mapping
  private val mappingDefinitions: mutable.Map[String, mutable.Map[String, FhirMapping]] = initMap(mappingRepositoryFolderPath)

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
   * @return the saved FhirMapping
   * @throws AlreadyExists if a mapping with the same ID or URL already exists
   */
  override def createMapping(projectId: String, mapping: FhirMapping): Future[FhirMapping] = {
    // validate that mapping id is unique
    if (mappingDefinitions.contains(projectId) && mappingDefinitions(projectId).contains(mapping.id)) {
      throw AlreadyExists("Fhir mapping already exists.", s"A mapping definition with id ${mapping.id} already exists in the mapping repository at ${FileUtils.getPath(mappingRepositoryFolderPath).toAbsolutePath.toString}")
    }
    // validate that mapping url is unique
    if (mappingDefinitions.contains(projectId) && mappingDefinitions(projectId).exists(mD => mD._2.url.contentEquals(mapping.url))) {
      throw AlreadyExists("Fhir mapping already exists.", s"A mapping definition with url ${mapping.url} already exists in the mapping repository at ${FileUtils.getPath(mappingRepositoryFolderPath).toAbsolutePath.toString}")
    }

    // Write to the repository as a new file
    getFileForMapping(projectId, mapping).map(newFile => {
      val fw = new FileWriter(newFile)
      fw.write(writePretty(mapping))
      fw.close()

      // Add to the project metadata json file
      projectFolderRepository.addMapping(projectId, mapping)
      // Add to the in-memory map
      mappingDefinitions.getOrElseUpdate(projectId, mutable.Map.empty).put(mapping.id, mapping)
      mapping
    })
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
   * @throws AlreadyExists if a mapping with the same URL already exists
   */
  override def putMapping(projectId: String, id: String, mapping: FhirMapping): Future[FhirMapping] = {
    // cross check ids
    if (!id.equals(mapping.id)) {
      throw BadRequest("Mapping definition is not valid.", s"Identifier of the mapping definition: ${mapping.id} does not match with explicit id: $id")
    }
    if (!mappingDefinitions.contains(projectId) || !mappingDefinitions(projectId).contains(id)) {
      throw ResourceNotFound("Mapping does not exists.", s"A mapping with id $id does not exists in the mapping repository at ${FileUtils.getPath(mappingRepositoryFolderPath).toAbsolutePath.toString}")
    }
    // validate that mapping url is unique
    if (mappingDefinitions.contains(projectId) && mappingDefinitions(projectId).exists(mD => !mD._1.contentEquals(id) && mD._2.url.contentEquals(mapping.url))) {
      throw AlreadyExists("Fhir mapping already exists.", s"A mapping definition with url ${mapping.url} already exists in the mapping repository at ${FileUtils.getPath(mappingRepositoryFolderPath).toAbsolutePath.toString}")
    }
    // update the mapping in the repository
    getFileForMapping(projectId, mapping).map(file => {
      val fw = new FileWriter(file)
      fw.write(writePretty(mapping))
      fw.close()
      // update the mapping in the in-memory map
      mappingDefinitions(projectId).put(id, mapping)
      // update the projects metadata json file
      projectFolderRepository.updateMapping(projectId, mapping)
      mapping
    })
  }

  /**
   * Delete the mapping from the repository
   *
   * @param projectId project id the mapping belongs to
   * @param id        mapping id
   * @return
   */
  override def deleteMapping(projectId: String, id: String): Future[Unit] = {
    if (!mappingDefinitions.contains(projectId) || !mappingDefinitions(projectId).contains(id)) {
      throw ResourceNotFound("Mapping does not exists.", s"A mapping with id $id does not exists in the mapping repository at ${FileUtils.getPath(mappingRepositoryFolderPath).toAbsolutePath.toString}")
    }

    // delete the mapping from the repository
    getFileForMapping(projectId, mappingDefinitions(projectId)(id)).map(file => {
      file.delete()
      mappingDefinitions(projectId).remove(id)
      // delete the mapping from projects json file
      projectFolderRepository.deleteMapping(projectId, Some(id))
    })
  }

  /**
   * Deletes all mappings associated with a specific project.
   *
   * @param projectId The unique identifier of the project for which mappings should be deleted.
   */
  override def deleteProjectMappings(projectId: String): Unit = {
    // delete mapping definitions for the project
    org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(mappingRepositoryFolderPath, projectId).toFile)
    // remove project from the cache
    mappingDefinitions.remove(projectId)
    // delete project mappings
    projectFolderRepository.deleteMapping(projectId)
  }

  /**
   * Retrieves the identifiers of mappings referencing the given schema in their definitions.
   * @param projectId identifier of project whose mappings will be checked
   * @param schemaUrl the url of schema
   * @return the identifiers of mappings referencing the given schema in their definitions
   */
  override def getMappingsReferencingSchema(projectId: String, schemaUrl: String): Future[Seq[String]] = {
    Future {
      mappingDefinitions.getOrElse(projectId,Map.empty) // handle the case where project has no mappings by returning an empty Map
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
  private def getFileForMapping(projectId: String, fhirMapping: FhirMapping): Future[File] = {
    val projectFuture: Future[Option[Project]] = projectFolderRepository.getProject(projectId)
    projectFuture.map(project => {
      val file: File = FileUtils.getPath(mappingRepositoryFolderPath, project.get.id, getFileName(fhirMapping.id)).toFile
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
        // Try to parse the file content as FhirMapping
        try {
          val fhirMapping = fileContent.parseJson.extract[FhirMapping]
          // discard if the mapping id and file name not match
          if (FileOperations.checkFileNameMatchesEntityId(fhirMapping.id, file, "mapping")) {
            fhirMappingMap.put(fhirMapping.id, fhirMapping)
          }
        }catch{
          case _: JsonParseException =>
            logger.error(s"Failed to parse '${file.getPath}'!")
            System.exit(1)
        }
      }
      map.put(projectDirectory.getName, fhirMappingMap)
    }
    map
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
    // nothing needs to be done as we keep the cache always up-to-date
  }
}
