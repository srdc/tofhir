package io.tofhir.server.service.project

import java.io.{File, FileWriter}
import com.typesafe.scalalogging.Logger
import io.onfhir.util.JsonFormatter.formats
import io.tofhir.engine.config.ToFhirEngineConfig
import io.tofhir.engine.model.{FhirMapping, FhirMappingJob}
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.model.{Project, SchemaDefinition}
import io.tofhir.server.service.job.JobFolderRepository
import io.tofhir.server.service.mapping.MappingFolderRepository
import io.tofhir.server.service.project.ProjectFolderRepository
import io.tofhir.server.service.schema.SchemaFolderRepository
import io.tofhir.server.util.FileOperations
import org.json4s.{JArray, JObject}
import scala.language.postfixOps
import scala.concurrent.duration.DurationInt
import io.tofhir.engine.Execution.actorSystem.dispatcher

import scala.collection.mutable
import scala.concurrent.{Await, Future}

/**
 * Folder/Directory based database initializer implementation.
 * */
class FolderDBInitializer(config: ToFhirEngineConfig,
                          schemaFolderRepository: SchemaFolderRepository,
                          mappingFolderRepository: MappingFolderRepository,
                          mappingJobFolderRepository: JobFolderRepository,
                          projectFolderRepository: ProjectFolderRepository) {

  private val logger: Logger = Logger(this.getClass)

  /**
   * Initializes the projects managed by this repository and populates the cache.
   */
  def init(): Unit = {

    val file = FileUtils.getPath(config.toFhirDbFolderPath, ProjectFolderRepository.PROJECTS_JSON).toFile

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
      file.getParentFile.mkdirs()
      file.createNewFile()
      // Parse the folder structure of the respective resource and to initialize projects with the resources found.
      val projects = initProjectsWithResources()
      if (projects.nonEmpty) {
        projectFolderRepository.updateProjectsMetadata()
      } else {
        // Initialize projects metadata file with empty array
        val fw = new FileWriter(file)
        try fw.write("[]") finally fw.close()
      }

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
}
