package io.tofhir.server.service

import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.model.FhirMappingJob
import io.tofhir.server.common.model.BadRequest
import io.tofhir.server.repository.job.IJobRepository

import javax.ws.rs.BadRequestException
import scala.concurrent.Future

class JobService(jobRepository: IJobRepository) extends LazyLogging {

  /**
   * Get all mapping metadata from the mapping repository
   *
   * @param projectId project id the mappings belong to
   * @return
   */
  def getAllMetadata(projectId: String): Future[Seq[FhirMappingJob]] = {
    jobRepository.getAllJobs(projectId)
  }

  /**
   * Create a new job
   *
   * @param projectId project id the job will belong to
   * @param job       job to create
   * @return
   * @throws BadRequest if the mapping job is not valid
   */
  def createJob(projectId: String, job: FhirMappingJob): Future[FhirMappingJob] = {
    try {
      // validate the mapping job definition
      job.validate()
      // create the job
      jobRepository.saveJob(projectId, job)
    } catch {
      case e: BadRequestException => throw BadRequest("Invalid mapping job!", e.getMessage)
    }
  }

  /**
   * Get the job by its id
   *
   * @param projectId project id the job belongs to
   * @param jobId     job id
   * @return
   */
  def getJob(projectId: String, jobId: String): Future[Option[FhirMappingJob]] = {
    jobRepository.getJob(projectId, jobId)
  }

  /**
   * Update the job
   *
   * @param projectId project id the job belongs to
   * @param jobId     job id
   * @param job       job to update
   * @return
   * @throws BadRequest when the mapping job is not valid
   */
  def updateJob(projectId: String, jobId: String, job: FhirMappingJob): Future[FhirMappingJob] = {
    if (!jobId.equals(job.id)) {
      throw BadRequest("Job definition is not valid.", s"Identifier of the job definition: ${job.id} does not match with the provided jobId: $jobId in the path!")
    }
    try {
      // validate the mapping job definition
      job.validate()
      // update the job
      jobRepository.updateJob(projectId, jobId, job)
    } catch {
      case e: BadRequestException => throw BadRequest("Invalid mapping job!", e.getMessage)
    }
  }

  /**
   * Delete the job
   *
   * @param projectId project id the job belongs to
   * @param jobId     job id
   * @return
   */
  def deleteJob(projectId: String, jobId: String): Future[Unit] = {
    jobRepository.deleteJob(projectId, jobId)
  }
}
