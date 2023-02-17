package io.tofhir.server.service.project

import com.typesafe.scalalogging.Logger
import io.onfhir.util.JsonFormatter._
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.config.ToFhirEngineConfig
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.model._
import io.tofhir.server.util.FileOperations
import org.json4s._
import org.json4s.jackson.Serialization.writePretty

import java.io.{File, FileWriter}
import scala.concurrent.Future

/**
 * Folder/Directory based project repository implementation.
 *
 * @param config Engine configs
 */
class ProjectFolderRepository(config: ToFhirEngineConfig) extends IProjectRepository {

  private val logger: Logger = Logger(this.getClass)

  /**
   * Retrieve all Projects
   *
   * @return all projects in the repository
   */
  override def getAllProjects: Future[Seq[Project]] = {
    Future {
      getProjectsMetadata()
    }
  }

  /**
   * Save project to the repository.
   *
   * @param project project to be saved
   * @return the created project
   */
  override def createProject(project: Project): Future[Project] = {
    Future {
      val projects = getProjectsMetadata()
      // validate that there is no project with the given id and name
      if (projects.exists(p => p.id.contentEquals(project.id))) {
        throw AlreadyExists("Project already exists.", s"Project with id ${project.id} already exists in the repository.")
      }
      if (projects.exists(p => p.name.contentEquals(project.name))) {
        throw AlreadyExists("Project already exists.", s"Project with name ${project.name} already exists in the repository.")
      }
      // update projects metadata file by adding the new project
      updateProjectsMetadata(projects :+ project)
      project
    }
  }

  /**
   * Retrieve the project identified by its id.
   *
   * @param id id of the project
   * @return the project
   */
  override def getProject(id: String): Future[Option[Project]] = {
    Future {
      val projects = getProjectsMetadata()
      projects.find(p => p.id.contentEquals(id))
    }
  }

  /**
   * Update the some fields of project in the repository.
   *
   * @param id    id of the project
   * @param patch patch to be applied to the project
   * @return
   */
  override def updateProject(id: String, patch: JObject): Future[Project] = {
    Future {
      val projects = getProjectsMetadata()
      // validate that the project exists
      val (project, remainingProjects) = projects.partition(p => p.id.contentEquals(id))
      if (project.isEmpty)
        throw ResourceNotFound("Project does not exist.", s"Project $id not found")
      // update the editable fields of project with new values
      val newDescription = (patch \ ProjectEditableFields.DESCRIPTION).extract[String]
      val updatedProject = project.head.copy(description = Some(newDescription))
      // update the projects metadata
      updateProjectsMetadata(remainingProjects :+ updatedProject)
      updatedProject
    }
  }

  /**
   * Delete the project from the repository.
   *
   * @param id id of the project
   * @return
   */
  override def removeProject(id: String): Future[Unit] = {
    Future {
      val projects = getProjectsMetadata()
      // validate that the project exists
      val (project, remainingProjects) = projects.partition(p => p.id.contentEquals(id))
      if (project.isEmpty)
        throw ResourceNotFound("Project does not exist.", s"Project $id not found")
      // update projects metadata with the remaining ones
      updateProjectsMetadata(remainingProjects)

      // Delete the schema, mappings and job folders the project
      org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(config.schemaRepositoryFolderPath, project.head.id).toFile)
      org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(config.contextPath, project.head.id).toFile)
      org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(config.mappingRepositoryFolderPath, project.head.id).toFile)
    }
  }

  /**
   * Adds the schema definition metadata to the project
   *
   * @param projectId
   * @param schemaMetadata
   */
  def addSchemaMetadata(projectId: String, schemaMetadata: SchemaDefinition): Unit = {
    val projects: Seq[Project] = getProjectsMetadata()
    val projectIndex: Int = projects.indexWhere(p => p.id.equals(projectId))
    val project: Project = projects(projectIndex)

    val updatedProjects: Seq[Project] = projects.updated(projectIndex, project.copy(schemas = project.schemas :+ schemaMetadata))
    updateProjectsMetadata(updatedProjects)
  }

  /**
   * Replaces the schema definition metadata of the project
   *
   * @param projectId
   * @param schemaMetadata
   */
  def updateSchemaMetadata(projectId: String, schemaMetadata: SchemaDefinition): Unit = {
    deleteSchemaMetadata(projectId, schemaMetadata.id)
    addSchemaMetadata(projectId, schemaMetadata)
  }

  /**
   * Deletes the schema definition metadata of the project
   *
   * @param projectId
   * @param schemaId
   */
  def deleteSchemaMetadata(projectId: String, schemaId: String): Unit = {
    val projects: Seq[Project] = getProjectsMetadata()
    val projectIndex: Int = projects.indexWhere(p => p.id.equals(projectId))
    val project: Project = projects(projectIndex)

    val updatedProjects: Seq[Project] = projects.updated(projectIndex, project.copy(schemas = project.schemas.filterNot(s => s.id.equals(schemaId))))
    updateProjectsMetadata(updatedProjects)
  }

  /**
   * Adds the mapping definition metadata to the project
   *
   * @param projectId Project id the mapping will be added to
   * @param mappingMetadata Mapping metadata to be added
   */
  def addMappingMetadata(projectId: String, mappingMetadata: MappingMetadata): Unit = {
    val projects: Seq[Project] = getProjectsMetadata()
    val projectIndex: Int = projects.indexWhere(p => p.id.equals(projectId))
    val project: Project = projects(projectIndex)

    val updatedProjects: Seq[Project] = projects.updated(projectIndex, project.copy(mappings = project.mappings :+ mappingMetadata))
    updateProjectsMetadata(updatedProjects)
  }

  /**
   * Replaces the mapping definition metadata of the project
   *
   * @param projectId Project id the mapping will be updated to
   * @param mappingMetadata Mapping metadata to be updated
   */
  def updateMappingMetadata(projectId: String, mappingMetadata: MappingMetadata): Unit = {
    deleteMappingMetadata(projectId, mappingMetadata.id)
    addMappingMetadata(projectId, mappingMetadata)
  }

  /**
   * Deletes the mapping definition metadata of the project
   *
   * @param projectId Project id the mapping will be deleted from
   * @param mappingId Mapping id to be deleted
   */
  def deleteMappingMetadata(projectId: String, mappingId: String): Unit = {
    val projects: Seq[Project] = getProjectsMetadata()
    val projectIndex: Int = projects.indexWhere(p => p.id.equals(projectId))
    val project: Project = projects(projectIndex)

    val updatedProjects: Seq[Project] = projects.updated(projectIndex, project.copy(mappings = project.mappings.filterNot(m => m.id.equals(mappingId))))
    updateProjectsMetadata(updatedProjects)
  }

  /**
   * Returns the projects in the repository by reading them from the projects metadata file.
   *
   * @return projects in the repository
   * */
  private def getProjectsMetadata(): Seq[Project] = {
    val file = FileUtils.findFileByName(config.toFhirDbFolderPath + File.separatorChar, ProjectFolderRepository.PROJECTS_JSON)
    file match {
      case Some(f) =>
        FileOperations.readJsonContent[Project](f)
      case None => {
        // when projects metadata file does not exist, create it
        logger.debug("There does not exist a metadata file for projects. Creating it...")
        val file = new File(config.toFhirDbFolderPath + File.separatorChar, ProjectFolderRepository.PROJECTS_JSON)
        file.createNewFile()
        // initialize projects metadata file with empty array
        val fw = new FileWriter(file)
        try fw.write("[]") finally fw.close()
        Seq.empty
      }
    }
  }

  /**
   * Updates the projects metadata file with the given projects.
   *
   * @param projects projects
   * */
  private def updateProjectsMetadata(projects: Seq[Project]) = {
    val file = new File(config.toFhirDbFolderPath + File.separatorChar, ProjectFolderRepository.PROJECTS_JSON)
    // when projects metadata file does not exist, create it
    if (!file.exists()) {
      logger.debug("There does not exist a metadata file for projects to update. Creating it...")
      file.createNewFile()
    }
    // write projects to the file
    val fw = new FileWriter(file)
    try fw.write(writePretty(projects)) finally fw.close()
  }
}

/**
 * Keeps file/folder names related to the project repository
 * */
object ProjectFolderRepository {
  val PROJECTS_JSON = "projects.json" // file keeping the metadata of all projects
}
