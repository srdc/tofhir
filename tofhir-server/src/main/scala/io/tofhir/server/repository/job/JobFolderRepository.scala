package io.tofhir.server.repository.job

import com.typesafe.scalalogging.Logger
import io.onfhir.api.util.IOUtil
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.model.FhirMappingJob
import io.tofhir.engine.util.FhirMappingJobFormatter.formats
import io.tofhir.engine.util.FileUtils
import io.tofhir.engine.util.FileUtils.FileExtensions
import io.tofhir.server.common.model.{AlreadyExists, BadRequest, ResourceNotFound}
import io.tofhir.server.model._
import io.tofhir.server.repository.project.ProjectFolderRepository
import io.tofhir.server.util.FileOperations
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.Serialization.writePretty

import java.io.{File, FileWriter}
import java.nio.charset.StandardCharsets
import scala.collection.mutable
import scala.concurrent.Future
import scala.io.Source

class JobFolderRepository(jobRepositoryFolderPath: String, projectFolderRepository: ProjectFolderRepository) extends IJobRepository {

  private val logger: Logger = Logger(this.getClass)
  // project id -> mapping job id -> mapping job
  private val jobDefinitions: mutable.Map[String, mutable.Map[String, FhirMappingJob]] = initMap(jobRepositoryFolderPath)

  /**
   * Returns the mappings managed by this repository
   *
   * @return
   */
  override def getCachedMappingsJobs: mutable.Map[String, mutable.Map[String, FhirMappingJob]] = {
    jobDefinitions
  }


  /**
   * Retrieve all jobs
   *
   * @param projectId project id the jobs belong to
   * @return
   */
  override def getAllJobs(projectId: String): Future[Seq[FhirMappingJob]] = {
    Future {
      if (jobDefinitions.contains(projectId)) {
        jobDefinitions(projectId).values.toSeq
      } else {
        Seq.empty
      }
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
    if (jobDefinitions.contains(projectId) && jobDefinitions(projectId).contains(job.id)) {
      throw AlreadyExists("Fhir mapping job already exists.", s"A job definition with id ${job.id} already exists in the job repository at ${FileUtils.getPath(jobRepositoryFolderPath).toAbsolutePath.toString}")
    }
    // Write to the repository as a new file
    getFileForJob(projectId, job.id).map(file => {
      val fw = new FileWriter(file)
      fw.write(writePretty(job))
      fw.close()
      // add the job to the project repo and the map
      projectFolderRepository.addJob(projectId, job)
      jobDefinitions.getOrElseUpdate(projectId, mutable.Map.empty).put(job.id, job)
      job
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
      jobDefinitions(projectId).get(jobId)
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
    if (!jobDefinitions.contains(projectId) || !jobDefinitions(projectId).contains(jobId)) {
      throw ResourceNotFound("Mapping job does not exists.", s"A mapping job with id $jobId does not exists in the mapping job repository at ${FileUtils.getPath(jobRepositoryFolderPath).toAbsolutePath.toString}")
    }
    // update the job in the repository
    getFileForJob(projectId, job.id).map(file => {
      val fw = new FileWriter(file)
      fw.write(writePretty(job))
      fw.close()
      // update the mapping job in the map
      jobDefinitions(projectId).put(jobId, job)
      // update the job in the project
      projectFolderRepository.updateJob(projectId, job)
      job
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
    if (!jobDefinitions.contains(projectId) || !jobDefinitions(projectId).contains(jobId)) {
      throw ResourceNotFound("Mapping job does not exists.", s"A mapping job with id $jobId does not exists in the mapping job repository at ${FileUtils.getPath(jobRepositoryFolderPath).toAbsolutePath.toString}")
    }

    // delete the mapping job from the repository
    getFileForJob(projectId, jobDefinitions(projectId)(jobId).id).map(file => {
      file.delete()
      jobDefinitions(projectId).remove(jobId)
      // delete the job from the project
      projectFolderRepository.deleteJob(projectId, Some(jobId))
    })
  }

  /**
   * Deletes all jobs associated with a specific project.
   *
   * @param projectId The unique identifier of the project for which jobs should be deleted.
   */
  override def deleteProjectJobs(projectId: String): Unit = {
    // delete job definitions for the project
    org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(jobRepositoryFolderPath, projectId).toFile)
    // remove project from the cache
    jobDefinitions.remove(projectId)
    // delete project jobs
    projectFolderRepository.deleteJob(projectId)
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
      val jobs: Seq[FhirMappingJob] = jobDefinitions.getOrElse(key = projectId, default = Map.empty).values.toSeq
      jobs.filter(job => job.mappings.map(mappingTask => mappingTask.mappingRef).contains(mappingUrl))
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
    projectFolderRepository.getProject(projectId) map { project =>
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
  private def initMap(jobRepositoryFolderPath: String): mutable.Map[String, mutable.Map[String, FhirMappingJob]] = {
    val map = mutable.Map.empty[String, mutable.Map[String, FhirMappingJob]]
    val jobRepositoryFolder = FileUtils.getPath(jobRepositoryFolderPath).toFile
    logger.info(s"Initializing the Mapping Repository from path ${jobRepositoryFolder.getAbsolutePath}.")
    if (!jobRepositoryFolder.exists()) {
      jobRepositoryFolder.mkdirs()
    }
    var projectDirectories = Seq.empty[File]
    projectDirectories = jobRepositoryFolder.listFiles.filter(_.isDirectory).toSeq
    projectDirectories.foreach { projectDirectory =>
      // job-id -> FhirMappingJob
      val fhirJobMap: mutable.Map[String, FhirMappingJob] = mutable.Map.empty
      val files = IOUtil.getFilesFromFolder(projectDirectory, withExtension = Some(FileExtensions.JSON.toString), recursively = Some(true))
      files.map { file =>
        val source = Source.fromFile(file, StandardCharsets.UTF_8.name()) // read the JSON file
        val fileContent = try source.mkString finally source.close()
        // Try to parse the file content as FhirMappingJob
        try {
          val job = JsonMethods.parse(fileContent).extract[FhirMappingJob]
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
      map.put(projectDirectory.getName, fhirJobMap)
    }
    map
  }
}
