package io.tofhir.server.service.job

import io.tofhir.engine.model.FhirMappingJob
import io.tofhir.server.model.JobMetadata

import scala.concurrent.Future

trait IJobRepository {

  /**
   * Retrieve the metadata of all jobs
   * @param projectId project id the jobs belong to
   * @return
   */
  def getAllJobMetadata(projectId: String): Future[Seq[JobMetadata]]

  /**
   * Save the job to the repository.
   * @param projectId project id the job belongs to
   * @param job job to save
   * @return
   */
  def createJob(projectId: String, job: FhirMappingJob): Future[FhirMappingJob]

  /**
   * Get the job by its id
   * @param projectId project id the job belongs to
   * @param id job id
   * @return
   */
  def getJob(projectId: String, id: String): Future[Option[FhirMappingJob]]

  /**
   * Update the job in the repository
   * @param projectId project id the job belongs to
   * @param id job id
   * @param job job to save
   * @return
   */
  def putJob(projectId: String, id: String, job: FhirMappingJob): Future[FhirMappingJob]

  /**
   * Delete the job from the repository
   * @param projectId project id the job belongs to
   * @param id job id
   * @return
   */
  def deleteJob(projectId: String, id: String): Future[Unit]

}
