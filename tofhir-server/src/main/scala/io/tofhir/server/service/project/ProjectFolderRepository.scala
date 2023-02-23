package io.tofhir.server.service.project

import com.typesafe.scalalogging.Logger
import io.onfhir.util.JsonFormatter._
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.config.ToFhirEngineConfig
import io.tofhir.engine.model.{FhirMapping, FhirMappingJob}
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.model._
import org.json4s._
import org.json4s.jackson.Serialization

import java.io.{File, FileWriter}
import scala.collection.mutable
import scala.concurrent.Future


/**
 * Folder/Directory based project repository implementation.
 *
 * @param config Engine configs
 */
class ProjectFolderRepository(config: ToFhirEngineConfig) extends IProjectRepository {

  private val logger: Logger = Logger(this.getClass)

  // Project cache keeping the up-to-date list of projects
  private var projects: mutable.Map[String, Project] = mutable.Map.empty

  /**
   * Initializes the projects cache
   *
   * @param projects
   */
  def setProjects(projects: mutable.Map[String, Project]): Unit = {
    this.projects = projects
  }

  /**
   * Retrieve all Projects
   *
   * @return all projects in the repository
   */
  override def getAllProjects: Future[Seq[Project]] = {
    Future {
      projects.values.toSeq.sortWith(projectComparisonFunc)
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
      // validate that there is no project with the given id and name
      if (projects.contains(project.id)) {
        throw AlreadyExists("Project already exists.", s"Project with id ${project.id} already exists in the repository.")
      }
      if (projects.values.exists(p => p.name.contentEquals(project.name))) {
        throw AlreadyExists("Project already exists.", s"Project with name ${project.name} already exists in the repository.")
      }

      // add project to the cache
      projects.put(project.id, project)

      // update projects metadata file by adding the new project
      updateProjectsMetadata()
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
      projects.get(id)
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
      // validate that the project exists
      if (!projects.contains(id))
        throw ResourceNotFound("Project does not exist.", s"Project $id not found")

      // update the editable fields of project with new values
      val newDescription = (patch \ ProjectEditableFields.DESCRIPTION).extract[String]
      val updatedProject = projects(id).copy(description = Some(newDescription))
      projects.put(id, updatedProject)

      // update the projects metadata
      updateProjectsMetadata()
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
      // validate that the project exists
      if (!projects.contains(id))
        throw ResourceNotFound("Project does not exist.", s"Project $id not found")

      // remove the project from the cache
      projects.remove(id)

      // update projects metadata with the remaining ones
      updateProjectsMetadata()

      // Delete the schema, mappings and job folders the project
      org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(config.schemaRepositoryFolderPath, id).toFile)
      org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(config.contextPath, id).toFile)
      org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(config.mappingRepositoryFolderPath, id).toFile)
      org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(config.jobRepositoryFolderPath, id).toFile)
    }
  }

  /**
   * Adds the schema definition to the project
   *
   * @param projectId
   * @param schema
   */
  def addSchema(projectId: String, schema: SchemaDefinition): Unit = {
    val project: Project = projects(projectId)
    projects.put(projectId, project.copy(schemas = project.schemas :+ schema))

    updateProjectsMetadata()
  }

  /**
   * Replaces the schema definition of the project
   *
   * @param projectId
   * @param schemaMetadata
   */
  def updateSchema(projectId: String, schemaMetadata: SchemaDefinition): Unit = {
    deleteSchema(projectId, schemaMetadata.id)
    addSchema(projectId, schemaMetadata)
  }

  /**
   * Deletes the schema definition of the project
   *
   * @param projectId
   * @param schemaId
   */
  def deleteSchema(projectId: String, schemaId: String): Unit = {
    val project: Project = projects(projectId)
    projects.put(projectId, project.copy(schemas = project.schemas.filterNot(s => s.id.equals(schemaId))))

    updateProjectsMetadata()
  }

  /**
   * Adds the mapping definition to the project json file
   *
   * @param projectId Project id the mapping will be added to
   * @param mapping Mapping to be added
   */
  def addMapping(projectId: String, mapping: FhirMapping): Unit = {
    val project: Project = projects(projectId)
    projects.put(projectId, project.copy(mappings = project.mappings :+ mapping))

    updateProjectsMetadata()
  }

  /**
   * Replaces the mapping definition in the project json file
   *
   * @param projectId Project id the mapping will be updated to
   * @param mapping Mapping to be updated
   */
  def updateMapping(projectId: String, mapping: FhirMapping): Unit = {
    deleteMapping(projectId, mapping.id)
    addMapping(projectId, mapping)
  }

  /**
   * Deletes the mapping definition from the project json file
   *
   * @param projectId Project id the mapping will be deleted from
   * @param mappingId Mapping id to be deleted
   */
  def deleteMapping(projectId: String, mappingId: String): Unit = {
    val project: Project = projects(projectId)
    projects.put(projectId, project.copy(mappings = project.mappings.filterNot(m => m.id.equals(mappingId))))

    updateProjectsMetadata()
  }

  /**
   * Adds the job definition to the project json file
   *
   * @param projectId
   * @param job
   */
  def addJob(projectId: String, job: FhirMappingJob): Unit = {
    val project: Project = projects(projectId)
    projects.put(projectId, project.copy(mappingJobs = project.mappingJobs :+ job))

    updateProjectsMetadata()
  }

  /**
   * Replaces the job definition of the project json file
   *
   * @param projectId
   * @param job
   */
  def updateJob(projectId: String, job: FhirMappingJob): Unit = {
    deleteJob(projectId, job.id)
    addJob(projectId, job)
  }

  /**
   * Deletes the job definition of the project json file
   *
   * @param projectId
   * @param jobId
   */
  def deleteJob(projectId: String, jobId: String): Unit = {
    val project: Project = projects(projectId)
    projects.put(projectId, project.copy(mappingJobs = project.mappingJobs.filterNot(j => j.id.equals(jobId))))

    updateProjectsMetadata()
  }

  /**
   * Updates the projects metadata with project included in the cache.
   */
  def updateProjectsMetadata() = {
    val file = new File(config.toFhirDbFolderPath + File.separatorChar, ProjectFolderRepository.PROJECTS_JSON)
    // when projects metadata file does not exist, create it
    if (!file.exists()) {
      logger.debug("There does not exist a metadata file for projects to update. Creating it...")
      file.createNewFile()
    }
    // write projects to the file. Only the metadata of the internal resources are written to the file
    val fw = new FileWriter(file)
    try fw.write(Serialization.writePretty(projects.values.map(_.getMetadata()).toList)) finally fw.close()
  }

  /**
   * Comparison function for two Projects. The definitions are compared according to their names with default String comparison.
   *
   * @param p1
   * @param p2
   * @return
   */
  private def projectComparisonFunc(p1: Project, p2: Project): Boolean = {
    p1.name.compareTo(p2.name) < 0
  }
}

/**
 * Keeps file/folder names related to the project repository
 * */
object ProjectFolderRepository {
  val PROJECTS_JSON = "projects.json" // file keeping the metadata of all projects
}
