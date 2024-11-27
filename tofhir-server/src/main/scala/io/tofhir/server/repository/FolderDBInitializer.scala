package io.tofhir.server.repository

import com.typesafe.scalalogging.Logger
import io.onfhir.definitions.common.model.Json4sSupport.formats
import io.onfhir.definitions.common.model.SchemaDefinition
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.model.{FhirMapping, FhirMappingJob}
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.model.Project
import io.tofhir.server.repository.job.JobFolderRepository
import io.tofhir.server.repository.mapping.ProjectMappingFolderRepository
import io.tofhir.server.repository.mappingContext.MappingContextFolderRepository
import io.tofhir.server.repository.project.ProjectFolderRepository
import io.tofhir.server.repository.schema.SchemaFolderRepository
import io.tofhir.server.util.FileOperations
import org.json4s.{JArray, JObject}

import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

/**
 * Folder/Directory based database initializer implementation.
 * */
class FolderDBInitializer(projectFolderRepository: ProjectFolderRepository,
                          schemaFolderRepository: SchemaFolderRepository,
                          mappingFolderRepository: ProjectMappingFolderRepository,
                          mappingJobFolderRepository: JobFolderRepository,
                          mappingContextRepository: MappingContextFolderRepository) {

  private val logger: Logger = Logger(this.getClass)

  /**
   * Initializes the projects managed by this repository and populates the cache.
   */
  def init(): Unit = {

    val file = FileUtils.getPath(ProjectFolderRepository.PROJECTS_JSON).toFile

    val parsedProjects = if (file.exists()) {
      val projects: JArray = FileOperations.readFileIntoJson(file).asInstanceOf[JArray]
      projects.arr.map(p => {
        val project: Project = initProjectFromMetadata(p.asInstanceOf[JObject])
        project.id -> project
      }).toMap
    } else {
      logger.debug(s"There does not exist a metadata file (${ProjectFolderRepository.PROJECTS_JSON}) for projects. Creating it...")
      file.createNewFile()
      // Parse the folder structure of the respective resource and to initialize projects with the resources found.
      val projects = initProjectsWithResources()
      projects
    }
    // Inject the parsed projects to the repository
    projectFolderRepository.setProjects(parsedProjects)
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
    val schemaUrlPrefix: Option[String] = (projectMetadata \ "schemaUrlPrefix").extractOpt[String]
    val mappingUrlPrefix: Option[String] = (projectMetadata \ "mappingUrlPrefix").extractOpt[String]
    val mappingContexts: Seq[String] = (projectMetadata \ "mappingContexts").extract[Seq[String]]
    // resolve schemas via the schema repository
    val schemaFutures: Future[Seq[Option[SchemaDefinition]]] = Future.sequence(
      (projectMetadata \ "schemas").asInstanceOf[JArray].arr.map(schemaMetadata => {
        val schemaId: String = (schemaMetadata \ "id").extract[String]
        schemaFolderRepository.getSchema(id, schemaId)
          .map {
            case None => throw new IllegalStateException(s"Failed to retrieve schema with id: $schemaId")
            case schema => schema
          }
      })
    )
    val schemas: Seq[SchemaDefinition] = Await.result[Seq[Option[SchemaDefinition]]](schemaFutures, 2 seconds).map(_.get)

    // resolve mappings via the mapping repository
    val mappingFutures: Future[Seq[Option[FhirMapping]]] = Future.sequence(
      (projectMetadata \ "mappings").asInstanceOf[JArray].arr.map(mappingMetadata => {
        val mappingId: String = (mappingMetadata \ "id").extract[String]
        mappingFolderRepository.getMapping(id, mappingId)
          .map {
            case None => throw new IllegalStateException(s"Failed to retrieve mapping with id: $mappingId")
            case mapping => mapping
          }
      })
    )
    val mappings: Seq[FhirMapping] = Await.result[Seq[Option[FhirMapping]]](mappingFutures, 2 seconds).map(_.get)

    // resolve mapping jobs via the mapping repository
    val mappingJobFutures: Future[Seq[Option[FhirMappingJob]]] = Future.sequence(
      (projectMetadata \ "mappingJobs").asInstanceOf[JArray].arr.map(jobMetadata => {
        val jobId: String = (jobMetadata \ "id").extract[String]
        mappingJobFolderRepository.getJob(id, jobId)
          .map {
            case None => throw new IllegalStateException(s"Failed to retrieve job with id: $jobId")
            case job => job
          }
      })
    )
    val jobs: Seq[FhirMappingJob] = Await.result[Seq[Option[FhirMappingJob]]](mappingJobFutures, 2 seconds).map(_.get)

    Project(id, name, description, schemaUrlPrefix, mappingUrlPrefix, schemas, mappings, mappingContexts, jobs)
  }

  private def dropLastPart(url: String): String = {
    // Remove trailing slash if it exists, to handle cases where URL ends with "/"
    val cleanedUrl = if (url.endsWith("/")) url.dropRight(1) else url
    // Find the last slash and drop everything after it
    val lastSlashIndex = cleanedUrl.lastIndexOf('/')
    if (lastSlashIndex != -1) cleanedUrl.substring(0, lastSlashIndex + 1)
    else url // Return original URL if no slashes found
  }

  /**
   * Parses the folder structure of project resources and initializes projects with the resources discovered.
   *
   * @return
   */
  private def initProjectsWithResources(): Map[String, Project] = {
    // Map keeping the projects. It uses the project name as a key.
    val projects: mutable.Map[String, Project] = mutable.Map.empty

    // Parse schemas
    schemaFolderRepository.getProjectPairs.foreach {
      case (projectId, schemaDefinitionList) =>
        val schemaUrl = schemaDefinitionList.head.url
        // If there is no project create a new one. Use id as name as well
        val project: Project = projects.get(projectId) match {
          case Some(existingProject) => existingProject.copy(schemaUrlPrefix = Some(dropLastPart(schemaUrl)))
          case None => Project(id = projectId, name = projectId, schemaUrlPrefix = Some(dropLastPart(schemaUrl)))
        }
        projects.put(projectId, project.copy(schemas = schemaDefinitionList))
    }

    // Parse mappings
    mappingFolderRepository.getProjectPairs.foreach {
      case (projectId, mappingList) =>
        val mappingUrl: String = mappingList.head.url
        // If there is no project create a new one. Use id as name as well
        val project: Project = projects.get(projectId) match {
          case Some(existingProject) => existingProject.copy(mappingUrlPrefix = Some(dropLastPart(mappingUrl)))
          case None => Project(id = projectId, name = projectId, mappingUrlPrefix = Some(dropLastPart(mappingUrl)))
        }
        projects.put(projectId, project.copy(mappings = mappingList))
    }

    // Parse mapping jobs
    mappingJobFolderRepository.getProjectPairs.foreach {
      case (projectId, jobList) =>
        // If there is no project create a new one. Use id as name as well
        val project: Project = projects.getOrElse(projectId, Project(id = projectId, name = projectId))
        projects.put(projectId, project.copy(mappingJobs = jobList))
    }

    // Parse mapping contexts
    mappingContextRepository.getProjectPairs.foreach {
      case (projectId, mappingContextIdList) =>
        // If there is no project create a new one. Use id as name as well
        val project: Project = projects.getOrElse(projectId, Project(id = projectId, name = projectId))
        projects.put(projectId, project.copy(mappingContexts = mappingContextIdList))
    }

    projects.toMap
  }

  /**
   * Removes the projects.json file.
   */
  def removeProjectsJsonFile(): Unit = {
    val file = FileUtils.getPath(ProjectFolderRepository.PROJECTS_JSON).toFile
    if (file.exists()) {
      file.delete()
    }
  }

}
