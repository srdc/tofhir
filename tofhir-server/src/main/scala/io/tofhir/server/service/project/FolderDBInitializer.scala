package io.tofhir.server.service.project

import com.typesafe.scalalogging.Logger
import io.tofhir.common.model.Json4sSupport.formats
import io.tofhir.common.model.SchemaDefinition
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.model.{FhirMapping, FhirMappingJob}
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.model.Project
import io.tofhir.server.service.job.JobFolderRepository
import io.tofhir.server.service.mapping.ProjectMappingFolderRepository
import io.tofhir.server.service.mappingcontext.MappingContextFolderRepository
import io.tofhir.server.service.schema.SchemaFolderRepository
import io.tofhir.server.util.FileOperations
import org.json4s.{JArray, JObject}

import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

/**
 * Folder/Directory based database initializer implementation.
 * */
class FolderDBInitializer(schemaFolderRepository: SchemaFolderRepository,
                          mappingFolderRepository: ProjectMappingFolderRepository,
                          mappingJobFolderRepository: JobFolderRepository,
                          projectFolderRepository: ProjectFolderRepository,
                          mappingContextRepository: MappingContextFolderRepository) {

  private val logger: Logger = Logger(this.getClass)

  /**
   * Initializes the projects managed by this repository and populates the cache.
   */
  def init(): Unit = {

    val file = FileUtils.getPath(ProjectFolderRepository.PROJECTS_JSON).toFile

    val parsedProjects = if (file.exists()) {
      val projects: JArray = FileOperations.readFileIntoJson(file).asInstanceOf[JArray]
      val projectMap: Map[String, Project] = projects.arr.map(p => {
        val project: Project = initProjectFromMetadata(p.asInstanceOf[JObject])
        project.id -> project
      })
        .toMap
      collection.mutable.Map(projectMap.toSeq: _*)
    } else {
      logger.debug("There does not exist a metadata file for projects. Creating it...")
      file.createNewFile()
      // Parse the folder structure of the respective resource and to initialize projects with the resources found.
      val projects = initProjectsWithResources()
      projects
    }
    // Inject the parsed projects to the repository
    projectFolderRepository.setProjects(parsedProjects)
    projectFolderRepository.updateProjectsMetadata() // update the metadata file after initialization
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
    val url: String = (projectMetadata \ "url").extract[String]
    val description: Option[String] = (projectMetadata \ "description").extractOpt[String]
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

    Project(id, name, url, description, schemas, mappings, mappingContexts, jobs)
  }

  /**
   * Parses the folder structure of project resources and initializes projects with the resources discovered.
   *
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
      val project: Project = projects.getOrElse(projectId, Project(projectId, projectId, convertToUrl(projectId), None))
      projects.put(projectId, project.copy(schemas = projectIdAndSchemas._2.values.toSeq))
    })

    // Parse mappings
    val mappings: mutable.Map[String, mutable.Map[String, FhirMapping]] = mappingFolderRepository.getCachedMappings()
    mappings.foreach(projectIdAndMappings => {
      val projectId: String = projectIdAndMappings._1
      // If there is no project create a new one. Use id as name as well
      val project: Project = projects.getOrElse(projectId, Project(projectId, projectId, convertToUrl(projectId), None))
      projects.put(projectId, project.copy(mappings = projectIdAndMappings._2.values.toSeq))
    })

    // Parse mapping jobs
    val jobs: mutable.Map[String, mutable.Map[String, FhirMappingJob]] = mappingJobFolderRepository.getCachedMappingsJobs
    jobs.foreach(projectIdAndMappingsJobs => {
      val projectId: String = projectIdAndMappingsJobs._1
      // If there is no project create a new one. Use id as name as well
      val project: Project = projects.getOrElse(projectId, Project(projectId, projectId, convertToUrl(projectId), None))
      projects.put(projectId, project.copy(mappingJobs = projectIdAndMappingsJobs._2.values.toSeq))
    })

    // Parse mapping contexts
    val mappingContexts: mutable.Map[String, Seq[String]] = mappingContextRepository.getCachedMappingContexts()
    mappingContexts.foreach(mappingContexts => {
      val projectId: String = mappingContexts._1
      // If there is no project create a new one. Use id as name as well
      val project: Project = projects.getOrElse(projectId, Project(projectId, projectId, convertToUrl(projectId), None))
      projects.put(projectId, project.copy(mappingContexts = mappingContexts._2))
    })

    projects
  }

  /**
   * Converts a project id to a url in a specific format.
   * @param inputString
   * @return
   */
  private def convertToUrl(inputString: String): String = {
    val transformedString = inputString.replaceAll("\\s", "").toLowerCase
    val url = s"https://www.$inputString.com"
    url
  }
}
