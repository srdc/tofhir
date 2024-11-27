package io.tofhir.server.repository.project

import com.typesafe.scalalogging.Logger
import io.onfhir.definitions.common.model.Json4sSupport.formats
import io.onfhir.definitions.common.model.SchemaDefinition
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.config.ToFhirEngineConfig
import io.tofhir.engine.model.{FhirMapping, FhirMappingJob}
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.common.model.{AlreadyExists, ResourceNotFound}
import io.tofhir.server.model._
import org.json4s._
import org.json4s.jackson.Serialization

import java.io.FileWriter
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
  private val projects: mutable.Map[String, Project] = mutable.Map.empty

  /**
   * Initializes the projects cache with the given Map of projects and persist to the file (projects.json)
   *
   * @param projects
   */
  def setProjects(vProjects: Map[String, Project]): Unit = {
    this.projects.clear()
    this.projects ++= vProjects
    this.updateProjectsMetadata()
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
   * Update some fields of the project in the repository.
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
      val newDescription = (patch \ ProjectEditableFields.DESCRIPTION).extractOpt[String]
      val newSchemaUrlPrefix = (patch \ ProjectEditableFields.SCHEMA_URL_PREFIX).extractOpt[String]
      val newMappingUrlPrefix = (patch \ ProjectEditableFields.MAPPING_URL_PREFIX).extractOpt[String]
      val updatedProject = projects(id).copy(description = newDescription, schemaUrlPrefix = newSchemaUrlPrefix, mappingUrlPrefix = newMappingUrlPrefix)
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
  override def deleteProject(id: String): Future[Unit] = {
    Future {
      // validate that the project exists
      if (!projects.contains(id))
        throw ResourceNotFound("Project does not exist.", s"Project $id not found")
      // remove the project from the cache
      projects.remove(id)
      // update projects metadata with the remaining ones
      updateProjectsMetadata()
    }
  }

  /**
   * Adds the schema definition to the project
   *
   * @param projectId
   * @param schema
   */

  override def addSchema(projectId: String, schema: SchemaDefinition): Future[Project] = {
    Future {
      val project: Project = projects(projectId)
      val updatedProject = project.copy(schemas = project.schemas :+ schema)
      projects.put(projectId, updatedProject)
      updateProjectsMetadata()
      updatedProject
    }
  }

  /**
   * Replaces the schema definition of the project
   *
   * @param projectId
   * @param schemaMetadata
   */
  override def updateSchema(projectId: String, schemaMetadata: SchemaDefinition): Future[Project] = {
    deleteSchema(projectId, Some(schemaMetadata.id)) flatMap { _ =>
      addSchema(projectId, schemaMetadata)
    }
  }

  /**
   * Deletes the schema definition (or all schema definitions) from the project.
   *
   * @param projectId
   * @param schemaId The unique identifier of the schema to be deleted. If not provided, all schemas will be removed from the project definition.
   */
  override def deleteSchema(projectId: String, schemaId: Option[String] = None): Future[Unit] = {
    Future {
      val project: Option[Project] = projects.get(projectId)
      project match {
        case Some(project) =>
          schemaId match {
            case Some(id) =>
              projects.put(projectId, project.copy(schemas = project.schemas.filterNot(s => s.id.equals(id))))
            case None =>
              projects.put(projectId, project.copy(schemas = Seq.empty))
          }
          updateProjectsMetadata()
        case None => // Do nothing if project is deleted before the schema
      }
    }
  }

  /**
   * Add the mapping definition to the project
   *
   * @param projectId Project id to which the mapping will be added to
   * @param mapping   Mapping to be added
   */
  override def addMapping(projectId: String, mapping: FhirMapping): Future[Project] = {
    Future {
      val project: Project = projects(projectId)
      val updatedProject = project.copy(mappings = project.mappings :+ mapping)
      projects.put(projectId, updatedProject)
      updateProjectsMetadata()
      updatedProject
    }
  }

  /**
   * Replaces the mapping definition within the project
   *
   * @param projectId Project id of which the mapping will be updated
   * @param mapping   Mapping to be updated
   */
  override def updateMapping(projectId: String, mapping: FhirMapping): Future[Project] = {
    deleteMapping(projectId, Some(mapping.id)) flatMap { _ =>
      addMapping(projectId, mapping)
    }
  }

  /**
   * Deletes the mapping definition from the
   *
   * @param projectId Project id from which the mapping will be deleted
   * @param mappingId The unique identifier of the mapping to be deleted. If not provided, all mappings for the project will be deleted.
   */
  override def deleteMapping(projectId: String, mappingId: Option[String] = None): Future[Unit] = {
    Future {
      val project: Option[Project] = projects.get(projectId)
      project match {
        case Some(project) =>
          mappingId match {
            case Some(id) =>
              projects.put(projectId, project.copy(mappings = project.mappings.filterNot(m => m.id.equals(id))))
            case None =>
              projects.put(projectId, project.copy(mappings = Seq.empty))
          }
          updateProjectsMetadata()
        case None => // Do nothing if project is deleted before the mapping
      }
    }
  }

  /**
   * Adds the job definition to the project
   *
   * @param projectId
   * @param job
   */
  override def addJob(projectId: String, job: FhirMappingJob): Future[Project] = {
    Future {
      val project: Project = projects(projectId)
      val updatedProject = project.copy(mappingJobs = project.mappingJobs :+ job)
      projects.put(projectId, updatedProject)
      updateProjectsMetadata()
      updatedProject
    }
  }

  /**
   * Replaces the job definition of the project json file
   *
   * @param projectId
   * @param job
   */
  override def updateJob(projectId: String, job: FhirMappingJob): Future[Project] = {
    deleteJob(projectId, Some(job.id)) flatMap { _ =>
      addJob(projectId, job)
    }
  }

  /**
   * Deletes the job definition of the project json file
   *
   * @param projectId
   * @param jobId The unique identifier of the job to be deleted. If not provided, all jobs for the project will be deleted.
   */
  override def deleteJob(projectId: String, jobId: Option[String] = None): Future[Unit] = {
    Future {
      val project: Option[Project] = projects.get(projectId)
      project match {
        case Some(project) =>
          jobId match {
            case Some(id) =>
              projects.put(projectId, project.copy(mappingJobs = project.mappingJobs.filterNot(j => j.id.equals(id))))
            case None =>
              projects.put(projectId, project.copy(mappingJobs = Seq.empty))
          }
          updateProjectsMetadata()
        case None => // Do nothing if project is deleted before the job
      }
    }
  }

  /**
   * Adds the mapping context id to the project json file
   *
   * @param projectId      Project id the mapping context will be added to
   * @param mappingContext Mapping context id to be added
   */
  override def addMappingContext(projectId: String, mappingContext: String): Future[Project] = {
    Future {
      val project: Project = projects(projectId)
      val updatedProject = project.copy(mappingContexts = project.mappingContexts :+ mappingContext)
      projects.put(projectId, updatedProject)
      updateProjectsMetadata()
      updatedProject
    }
  }

  /**
   * Deletes the mapping context in the project json file
   *
   * @param projectId        Project id the mapping context will be deleted
   * @param mappingContextId The unique identifier of the mapping context to be deleted. If not provided, all mapping contexts for the project will be deleted.
   */
  override def deleteMappingContext(projectId: String, mappingContextId: Option[String] = None): Future[Unit] = {
    Future {
      val project: Option[Project] = projects.get(projectId)
      project match {
        case Some(project) =>
          mappingContextId match {
            case Some(id) =>
              projects.put(projectId, project.copy(mappingContexts = project.mappingContexts.filterNot(m => m.equals(id))))
            case None =>
              projects.put(projectId, project.copy(mappingContexts = Seq.empty))
          }
          updateProjectsMetadata()
        case None => // Do nothing if project is deleted before the mapping context
      }
    }
  }

  /**
   * Updates the projects metadata with project included in the cache.
   */
  private def updateProjectsMetadata(): Unit = {
    val file = FileUtils.getPath(ProjectFolderRepository.PROJECTS_JSON).toFile
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
