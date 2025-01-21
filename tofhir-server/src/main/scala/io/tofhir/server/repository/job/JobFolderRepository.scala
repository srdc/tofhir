package io.tofhir.server.repository.job

import com.typesafe.scalalogging.Logger
import io.onfhir.api.util.IOUtil
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.model.FhirMappingJob
import io.tofhir.engine.util.FhirMappingJobFormatter.formats
import io.tofhir.engine.util.FileUtils.FileExtensions
import io.tofhir.engine.util.{FhirMappingJobFormatter, FileUtils}
import io.tofhir.server.common.model.{AlreadyExists, ResourceNotFound}
import io.tofhir.server.repository.project.IProjectRepository
import io.tofhir.server.util.FileOperations
import org.json4s.MappingException
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.Serialization.writePretty

import java.io.{File, FileWriter}
import java.nio.charset.StandardCharsets
import scala.collection.mutable
import scala.concurrent.Future
import scala.io.Source

class JobFolderRepository(jobRepositoryFolderPath: String, projectRepository: IProjectRepository) extends IJobRepository {

  private val logger: Logger = Logger(this.getClass)

  // In-memory cache to maintain the job definitions.
  // project id -> mapping job id -> mapping job
  private val jobDefinitions: mutable.Map[String, mutable.Map[String, FhirMappingJob]] = mutable.Map.empty[String, mutable.Map[String, FhirMappingJob]]

  // Initialize the map for the first time
  initMap(jobRepositoryFolderPath)

  /**
   * Retrieve all jobs of a given project
   *
   * @param projectId project id the jobs belong to
   * @return
   */
  override def getAllJobs(projectId: String): Future[Seq[FhirMappingJob]] = {
    Future {
      jobDefinitions.get(projectId)
        .map(_.values.toSeq) // If such a project exists, return the jobs as a sequence
        .getOrElse(Seq.empty[FhirMappingJob]) // Else, return an empty list
    }
  }

  /**
   * Save the job to the repository.
   *
   * @param projectId project id the job belongs to
   * @param job       job to save
   * @return
   */
  override def saveJob(projectId: String, job: FhirMappingJob): Future[FhirMappingJob] = {
    jobDefinitions.get(projectId).flatMap(_.get(job.id)).foreach { _ =>
      throw AlreadyExists("Fhir mapping job already exists.", s"A job definition with id ${job.id} already exists in the job repository at ${FileUtils.getPath(jobRepositoryFolderPath).toAbsolutePath.toString}")
    }
    // Write to the repository as a new file
    getFileForJob(projectId, job.id).flatMap(file => {
      val fw = new FileWriter(file)
      fw.write(writePretty(job))
      fw.close()
      // Update the internal cache of job repository
      jobDefinitions.getOrElseUpdate(projectId, mutable.Map.empty[String, FhirMappingJob]).put(job.id, job)
      // add the job to the project repository
      projectRepository.addJob(projectId, job) map { _ =>
        job
      }
    })
  }

  /**
   * Get the job by its id
   *
   * @param projectId project id the job belongs to
   * @param jobId     job id
   * @return
   */
  override def getJob(projectId: String, jobId: String): Future[Option[FhirMappingJob]] = {
    Future {
      jobDefinitions.get(projectId).flatMap(_.get(jobId))
    }
  }

  /**
   * Update the job in the repository
   *
   * @param projectId project id the job belongs to
   * @param jobId     job id
   * @param job       job to save
   * @return
   */
  override def updateJob(projectId: String, jobId: String, job: FhirMappingJob): Future[FhirMappingJob] = {
    if (!jobDefinitions.get(projectId).exists(_.contains(jobId))) {
      throw ResourceNotFound("Mapping job does not exists.", s"A mapping job with id $jobId does not exists in the mapping job repository at ${FileUtils.getPath(jobRepositoryFolderPath).toAbsolutePath.toString}")
    }
    // update the job in the repository
    getFileForJob(projectId, job.id).flatMap(file => {
      val fw = new FileWriter(file)
      fw.write(writePretty(job))
      fw.close()
      // update the mapping job in the map
      jobDefinitions(projectId).put(jobId, job)
      // update the job in the project
      projectRepository.updateJob(projectId, job) map { _ =>
        job
      }
    })
  }

  /**
   * Delete the job from the repository
   *
   * @param projectId project id the job belongs to
   * @param jobId     job id
   * @return
   */
  override def deleteJob(projectId: String, jobId: String): Future[Unit] = {
    if (!jobDefinitions.get(projectId).exists(_.contains(jobId))) {
      throw ResourceNotFound("Mapping job does not exists.", s"A mapping job with id $jobId does not exists in the mapping job repository at ${FileUtils.getPath(jobRepositoryFolderPath).toAbsolutePath.toString}")
    }

    // delete the mapping job from the repository
    getFileForJob(projectId, jobDefinitions(projectId)(jobId).id).flatMap(file => {
      file.delete()
      jobDefinitions(projectId).remove(jobId)
      // delete the job from the project
      projectRepository.deleteJob(projectId, Some(jobId))
    })
  }

  /**
   * Deletes all jobs associated with a specific project.
   *
   * @param projectId The unique identifier of the project for which jobs should be deleted.
   */
  override def deleteAllJobs(projectId: String): Future[Unit] = {
    Future {
      // delete job definitions for the project
      org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(jobRepositoryFolderPath, projectId).toFile)
      // remove project from the cache
      jobDefinitions.remove(projectId)
    } flatMap { _ =>
      // delete project jobs
      projectRepository.deleteJob(projectId)
    }
  }

  /**
   * Retrieves the jobs referencing the given mapping in their definitions.
   *
   * @param projectId  identifier of project whose jobs will be checked
   * @param mappingUrl the url of mapping
   * @return the jobs referencing the given mapping in their definitions
   */
  override def getJobsReferencingMapping(projectId: String, mappingUrl: String): Future[Seq[FhirMappingJob]] = {
    Future {
      val jobs = jobDefinitions.get(projectId).toSeq.flatMap(_.values)
      jobs.filter(_.mappings.exists(_.mappingRef == mappingUrl))
    }
  }

  /**
   * Get the mapping job file for the given project id and job id
   *
   * @param projectId
   * @param fhirMapping
   * @return
   */
  private def getFileForJob(projectId: String, jobId: String): Future[File] = {
    projectRepository.getProject(projectId) map { project =>
      if (project.isEmpty) throw new IllegalStateException(s"This should not be possible. ProjectId: $projectId does not exist in the project folder repository.")
      FileOperations.getFileForEntityWithinProject(jobRepositoryFolderPath, project.get.id, jobId)
    }
  }

  /**
   * Initialize the job definitions from the given folder
   *
   * @param jobRepositoryFolderPath folder path to the job repository
   * @return
   */
  private def initMap(jobRepositoryFolderPath: String): Unit = {
    val jobRepositoryFolder = FileUtils.getPath(jobRepositoryFolderPath).toFile
    logger.info(s"Initializing the Mapping Job Repository from path ${jobRepositoryFolder.getAbsolutePath}.")
    if (!jobRepositoryFolder.exists()) {
      jobRepositoryFolder.mkdirs()
    }
    var projectDirectories = Seq.empty[File]
    projectDirectories = jobRepositoryFolder.listFiles.filter(_.isDirectory).toSeq
    projectDirectories.foreach { projectDirectory =>
      // job-id -> FhirMappingJob
      val fhirJobMap: mutable.Map[String, FhirMappingJob] = mutable.Map.empty
      val files = IOUtil.getFilesFromFolder(projectDirectory, recursively = true, ignoreHidden = true, withExtension = Some(FileExtensions.JSON.toString))
      files.map { file =>
        val source = Source.fromFile(file, StandardCharsets.UTF_8.name()) // read the JSON file
        val fileContent = try source.mkString finally source.close()
        // Try to parse the file content as FhirMappingJob
        try {
          val job = JsonMethods.parse(fileContent).extract[FhirMappingJob]
          // check there are no duplicate name on mappingTasks of the job
          val duplicateMappingTasks = FhirMappingJobFormatter.findDuplicateMappingTaskNames(job.mappings)
          if (duplicateMappingTasks.nonEmpty) {
            throw new MappingException(s"Duplicate 'name' fields detected in the MappingTasks of job '${job.id}': ${duplicateMappingTasks.mkString(", ")}. Please ensure that each MappingTask has a unique name.")
          }
          // discard if the job id and file name not match
          if (FileOperations.checkFileNameMatchesEntityId(job.id, file, "job")) {
            fhirJobMap.put(job.id, job)
          }
        } catch {
          case e: Throwable =>
            logger.error(s"Failed to parse mapping job definition at ${file.getPath}", e)
            System.exit(1)
        }
      }
      this.jobDefinitions.put(projectDirectory.getName, fhirJobMap)
    }
  }

  /**
   * Reload the job definitions from the given folder
   *
   * @return
   */
  override def invalidate(): Unit = {
    this.jobDefinitions.clear()
    initMap(jobRepositoryFolderPath)
  }

  /**
   * Retrieve the projects and FhirMappingJobs within.
   *
   * @return Map of projectId -> Seq[FhirMappingJob]
   */
  override def getProjectPairs: Map[String, Seq[FhirMappingJob]] = {
    jobDefinitions.map { case (projectId, jobPairs) =>
      projectId -> jobPairs.values.toSeq
    }.toMap
  }
}
