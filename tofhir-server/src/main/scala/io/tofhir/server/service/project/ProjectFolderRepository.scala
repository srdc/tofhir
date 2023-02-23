package io.tofhir.server.service.project

import com.typesafe.scalalogging.Logger
import io.onfhir.util.JsonFormatter._
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.config.ToFhirEngineConfig
import io.tofhir.engine.model.{FhirMapping, FhirMappingJob}
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.model._
import io.tofhir.server.service.job.JobFolderRepository
import io.tofhir.server.service.mapping.MappingFolderRepository
import io.tofhir.server.service.schema.SchemaFolderRepository
import io.tofhir.server.util.FileOperations
import org.json4s._
import org.json4s.jackson.Serialization

import java.io.{File, FileWriter}
import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

/**
 * Folder/Directory based project repository implementation.
 *
 * @param config Engine configs
 */
class ProjectFolderRepository(config: ToFhirEngineConfig) extends IProjectRepository {

  private val logger: Logger = Logger(this.getClass)

  // Project cache keeping the up-to-date list of projects
  private var projects: mutable.Map[String, Project] = mutable.Map.empty

  // The repository instances are used during the initialization process to resolve resources referred by projects.
  private var schemaFolderRepository: SchemaFolderRepository = _
  private var mappingFolderRepository: MappingFolderRepository = _
  private var mappingJobFolderRepository: JobFolderRepository = _

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
  private def updateProjectsMetadata() = {
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
   * Initializes the projects managed by this repository and populates the cache.
   *
   * @param schemaFolderRepository
   * @param mappingFolderRepository
   * @param mappingJobFolderRepository
   */
  def initProjects(schemaFolderRepository: SchemaFolderRepository, mappingFolderRepository: MappingFolderRepository, mappingJobFolderRepository: JobFolderRepository): Unit = {
    this.schemaFolderRepository = schemaFolderRepository
    this.mappingFolderRepository = mappingFolderRepository
    this.mappingJobFolderRepository = mappingJobFolderRepository

    val file = FileUtils.findFileByName(config.toFhirDbFolderPath + File.separatorChar, ProjectFolderRepository.PROJECTS_JSON)
    file match {
      // A project json file exists. Initialize each project by resolving each resources referred by the project.
      case Some(f) =>
        val projects: JArray = FileOperations.readFileIntoJson(f).asInstanceOf[JArray]
          val projectMap: Map[String, Project] = projects.arr.map(p => {
            val project: Project = initProjectFromMetadata(p.asInstanceOf[JObject])
            project.id -> project
          })
            .toMap
          collection.mutable.Map(projectMap.toSeq: _*)

        // There is no project json metadata, create it
      case None => {
        logger.debug("There does not exist a metadata file for projects. Creating it...")
        val file = new File(config.toFhirDbFolderPath + File.separatorChar, ProjectFolderRepository.PROJECTS_JSON)
        file.createNewFile()

        // Parse the folder structure of the respective resource and to initialize projects with the resources found.
        val projects = initProjectsWithResources()
        if (projects.nonEmpty) {
          updateProjectsMetadata()

        } else {
          // Initialize projects metadata file with empty array
          val fw = new FileWriter(file)
          try fw.write("[]") finally fw.close()
        }

        this.projects = projects
      }
    }
  }

  /**
   * Constructs a project based on project metadata stored in the project json file. Resources are resolved via respective repositories.
   *
   * @param projectMetadata
   * @return
   */
  private def initProjectFromMetadata(projectMetadata: JObject): Project = {
    val id: String = (projectMetadata \ "id").extract[String]
    val name: String = (projectMetadata \ "name").extract[String]
    val description: Option[String] = (projectMetadata \ "description").extractOpt[String]
    val contextConceptMaps: Seq[String] = (projectMetadata \ "contextConceptMaps").extract[Seq[String]]
    // resolve schemas via the schema repository
    val schemaFutures: Future[Seq[Option[SchemaDefinition]]] = Future.sequence(
      (projectMetadata \ "schemas").asInstanceOf[JArray].arr.map(schemaMetadata => {
        schemaFolderRepository.getSchema(id, (schemaMetadata \ "id").extract[String])
      })
    )
    val schemas: Seq[SchemaDefinition] = Await.result[Seq[Option[SchemaDefinition]]](schemaFutures, 2 seconds).map(_.get)

    // resolve mappings via the mapping repository
    val mappingFutures: Future[Seq[Option[FhirMapping]]] = Future.sequence(
      (projectMetadata \ "mappings").asInstanceOf[JArray].arr.map(mappingMetadata => {
        mappingFolderRepository.getMapping(id, (mappingMetadata \ "id").extract[String])
      })
    )
    val mappings: Seq[FhirMapping] = Await.result[Seq[Option[FhirMapping]]](mappingFutures, 2 seconds).map(_.get)

    // resolve mapping jobs via the mapping repository
    val mappingJobFutures: Future[Seq[Option[FhirMappingJob]]] = Future.sequence(
      (projectMetadata \ "schemas").asInstanceOf[JArray].arr.map(jobMetadata => {
        mappingJobFolderRepository.getJob(id, (jobMetadata \ "id").extract[String])
      })
    )
    val jobs: Seq[FhirMappingJob] = Await.result[Seq[Option[FhirMappingJob]]](mappingJobFutures, 2 seconds).map(_.get)

    Project(id, name, description, schemas, mappings, contextConceptMaps, jobs)
  }

  /**
   * Parses the folder structure of project resources and initializes projects with the resources discovered.
   * @return
   */
  private def initProjectsWithResources(): mutable.Map[String, Project] = {
    // Map keeping the projects. It uses the project name as a key.
    val projects: mutable.Map[String, Project] = mutable.Map.empty

    // Parse schemas
    val schemas: mutable.Map[String, mutable.Map[String, SchemaDefinition]] = schemaFolderRepository.getCachedSchemas()
    schemas.foreach(projectIdAndSchemas => {
      val projectId: String = projectIdAndSchemas._1
      // If there is no project create a new one. Use id as name as well
      val project: Project = projects.getOrElse(projectId, Project(projectId, projectId, None))
      projects.put(projectId, project.copy(schemas = projectIdAndSchemas._2.values.toSeq))
    })

    // Parse mappings
    val mappings: mutable.Map[String, mutable.Map[String, FhirMapping]] = mappingFolderRepository.getCachedMappings()
    mappings.foreach(projectIdAndMappings => {
      val projectId: String = projectIdAndMappings._1
      // If there is no project create a new one. Use id as name as well
      val project: Project = projects.getOrElse(projectId, Project(projectId, projectId, None))
      projects.put(projectId, project.copy(mappings = projectIdAndMappings._2.values.toSeq))
    })

    // Parse mapping jobs
    val jobs: mutable.Map[String, mutable.Map[String, FhirMappingJob]] = mappingJobFolderRepository.getCachedMappingsJobs()
    jobs.foreach(projectIdAndMappingsJobs => {
      val projectId: String = projectIdAndMappingsJobs._1
      // If there is no project create a new one. Use id as name as well
      val project: Project = projects.getOrElse(projectId, Project(projectId, projectId, None))
      projects.put(projectId, project.copy(mappingJobs = projectIdAndMappingsJobs._2.values.toSeq))
    })

    // Parse concept maps
    // TODO parse concept maps

    projects
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
