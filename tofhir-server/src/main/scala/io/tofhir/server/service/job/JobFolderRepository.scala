package io.tofhir.server.service.job

import io.onfhir.api.util.IOUtil
import io.onfhir.util.JsonFormatter._
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.model.{FhirMappingException, FhirMappingJob}
import io.tofhir.engine.util.FileUtils
import io.tofhir.engine.util.FileUtils.FileExtensions
import io.tofhir.server.model.{AlreadyExists, BadRequest, JobMetadata, Project, ResourceNotFound}
import io.tofhir.server.service.project.ProjectFolderRepository
import org.json4s.jackson.Serialization.writePretty

import java.io.{File, FileWriter}
import java.nio.charset.StandardCharsets
import scala.collection.mutable
import scala.concurrent.Future
import scala.io.Source
import io.tofhir.engine.util.FhirMappingJobFormatter.formats

class JobFolderRepository(jobRepositoryFolderPath: String, projectFolderRepository: ProjectFolderRepository) extends IJobRepository {
  // project id -> mapping job id -> mapping job
  private val jobDefinitions: mutable.Map[String, mutable.Map[String, FhirMappingJob]] = initMap(jobRepositoryFolderPath)

  /**
   * Retrieve the metadata of all jobs
   *
   * @param projectId project id the jobs belong to
   * @return
   */
  override def getAllJobMetadata(projectId: String): Future[Seq[JobMetadata]] = {
    Future {
      if (jobDefinitions.contains(projectId)) {
        jobDefinitions(projectId).values.map(getJobMetadata).toSeq
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
  override def createJob(projectId: String, job: FhirMappingJob): Future[FhirMappingJob] = {
    Future {
      if (jobDefinitions.contains(projectId) && jobDefinitions(projectId).contains(job.id)) {
        throw AlreadyExists("Fhir mapping job already exists.", s"A job definition with id ${job.id} already exists in the job repository at ${FileUtils.getPath(jobRepositoryFolderPath).toAbsolutePath.toString}")
      }
      // Write to the repository as a new file
      getFileForJob(projectId, job).map(file => {
        val fw = new FileWriter(file)
        fw.write(writePretty(job))
        fw.close()
      })
      projectFolderRepository.addJobMetadata(projectId, getJobMetadata(job))
      jobDefinitions.getOrElseUpdate(projectId, mutable.Map.empty).put(job.id, job)
      job
    }
  }

  /**
   * Get the job by its id
   *
   * @param projectId project id the job belongs to
   * @param id        job id
   * @return
   */
override def getJob(projectId: String, id: String): Future[Option[FhirMappingJob]] = {
  Future {
    jobDefinitions(projectId).get(id)
  }
}

  /**
   * Update the job in the repository
   *
   * @param projectId project id the job belongs to
   * @param id        job id
   * @param job       job to save
   * @return
   */
  override def putJob(projectId: String, id: String, job: FhirMappingJob): Future[FhirMappingJob] = {
    Future {
      if (!id.equals(job.id)) {
        throw BadRequest("Job definition is not valid.", s"Identifier of the job definition: ${job.id} does not match with explicit id: $id")
      }
      if (!jobDefinitions.contains(projectId) || !jobDefinitions(projectId).contains(id)) {
        throw ResourceNotFound("Mapping job does not exists.", s"A mapping job with id $id does not exists in the mapping job repository at ${FileUtils.getPath(jobRepositoryFolderPath).toAbsolutePath.toString}")
      }
      // update the job in the repository
      getFileForJob(projectId, job).map(file => {
        val fw = new FileWriter(file)
        fw.write(writePretty(job))
        fw.close()
      })
      // update the mapping job in the map
      jobDefinitions(projectId).put(id, job)
      // update the metadata
      projectFolderRepository.updateJobMetadata(projectId, getJobMetadata(job))
      job
    }

  }

  /**
   * Delete the job from the repository
   *
   * @param projectId project id the job belongs to
   * @param id        job id
   * @return
   */
  override def deleteJob(projectId: String, id: String): Future[Unit] = {
    Future {
      if (!jobDefinitions.contains(projectId) || !jobDefinitions(projectId).contains(id)) {
        throw ResourceNotFound("Mapping job does not exists.", s"A mapping job with id $id does not exists in the mapping job repository at ${FileUtils.getPath(jobRepositoryFolderPath).toAbsolutePath.toString}")
      }
      // delete the mapping job from the repository
      getFileForJob(projectId, jobDefinitions(projectId)(id)).map(file => {
        file.delete()
      })
      // delete the mapping job from the map
      jobDefinitions(projectId).remove(id)
      // delete the metadata
      projectFolderRepository.deleteJobMetadata(projectId, id)
    }
  }

  /**
   * Get the mapping job file for the given project id and job id
   * @param projectId
   * @param fhirMapping
   * @return
   */
  private def getFileForJob(projectId: String, fhirMapping: FhirMappingJob): Future[File] = {
    val projectFuture: Future[Option[Project]] = projectFolderRepository.getProject(projectId)
    projectFuture.map(project => {
      val file: File = FileUtils.getPath(jobRepositoryFolderPath, project.get.id, getFileName(fhirMapping.id)).toFile
      // If the project folder does not exist, create it
      if (!file.getParentFile.exists()) {
        file.getParentFile.mkdir()
      }
      file
    })
  }

  /**
   * Get the file name of the given job
   * @param jobId job id
   * @return
   */
  private def getFileName(jobId: String): String = {
    s"$jobId${FileExtensions.JSON}"
  }

  /**
   * Get the metadata of the given job
   * @param job job to get the metadata
   * @return
   */
  private def getJobMetadata(job: FhirMappingJob): JobMetadata = {
    JobMetadata(job.id, job.name.getOrElse(job.id))
  }

  /**
   * Initialize the job definitions from the given folder
   * @param jobRepositoryFolderPath folder path to the job repository
   * @return
   */
  private def initMap(jobRepositoryFolderPath: String): mutable.Map[String, mutable.Map[String, FhirMappingJob]] = {
    val map = mutable.Map.empty[String, mutable.Map[String, FhirMappingJob]]
    val folder = FileUtils.getPath(jobRepositoryFolderPath).toFile
    var directories = Seq.empty[File]
    try {
      directories = folder.listFiles.filter(_.isDirectory).toSeq
    } catch {
      case e: Throwable => throw FhirMappingException(s"Given folder for the mapping job repository is not valid.", e)
    }
    // job-id -> FhirMappingJob
    val fhirJobMap: mutable.Map[String, FhirMappingJob] = mutable.Map.empty
    directories.foreach { projectDirectory =>
      val files = IOUtil.getFilesFromFolder(projectDirectory, withExtension = Some(FileExtensions.JSON.toString), recursively = Some(true))
      files.map { file =>
        val source = Source.fromFile(file, StandardCharsets.UTF_8.name()) // read the JSON file
        val fileContent = try source.mkString finally source.close()
        val job = fileContent.parseJson.extract[FhirMappingJob]
        fhirJobMap.put(job.id, job)
      }
      map.put(projectDirectory.getName, fhirJobMap)
    }
    map
  }
}
