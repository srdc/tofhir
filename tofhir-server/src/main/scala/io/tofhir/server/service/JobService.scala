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
   * @param projectId project id the mappings belong to
   * @return
   */
  def getAllMetadata(projectId: String): Future[Seq[FhirMappingJob]] = {
    jobRepository.getAllJobs(projectId)
  }

  /**
   * Create a new job
   * @param projectId project id the job will belong to
   * @param job job to create
   * @return
   * @throws BadRequest if the mapping job is not valid
   */
  def createJob(projectId: String, job: FhirMappingJob): Future[FhirMappingJob] = {
    try{
      // validate the mapping job definition
      job.validate()
      // create the job
      jobRepository.createJob(projectId, job)
    } catch {
      case _: BadRequestException => throw BadRequest("Invalid mapping job!","Streaming jobs cannot be scheduled.")
    }
  }

  /**
   * Get the job by its id
   * @param projectId project id the job belongs to
   * @param jobId job id
   * @return
   */
  def getJob(projectId: String, jobId: String): Future[Option[FhirMappingJob]] = {
    jobRepository.getJob(projectId, jobId)
  }

  /**
   * Update the job
   * @param projectId project id the job belongs to
   * @param id job id
   * @param job job to update
   * @return
   * @throws BadRequest when the mapping job is not valid
   */
  def updateJob(projectId: String, id: String, job: FhirMappingJob): Future[FhirMappingJob] = {
    try{
      // validate the mapping job definition
      job.validate()
      // update the job
      jobRepository.putJob(projectId, id, job)
    } catch {
      case _: BadRequestException => throw BadRequest("Invalid mapping job!", "Streaming jobs cannot be scheduled.")
    }
  }

  /**
   * Delete the job
   * @param projectId project id the job belongs to
   * @param id job id
   * @return
   */
  def deleteJob(projectId: String, id: String): Future[Unit] = {
    jobRepository.deleteJob(projectId, id)
  }
}
