package io.tofhir.server.service

import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.model.FhirMappingJob
import io.tofhir.server.service.job.{IJobRepository, JobFolderRepository}
import io.tofhir.server.service.project.{IProjectRepository, ProjectFolderRepository}

import scala.concurrent.Future;

class JobService(jobRepository: IJobRepository, projectRepository: IProjectRepository) extends LazyLogging {

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
   */
  def createJob(projectId: String, job: FhirMappingJob): Future[FhirMappingJob] = {
    jobRepository.createJob(projectId, job)
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
   */
  def updateJob(projectId: String, id: String, job: FhirMappingJob): Future[FhirMappingJob] = {
    jobRepository.putJob(projectId, id, job)
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
