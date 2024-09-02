package io.tofhir.server.repository.job

import io.tofhir.engine.model.FhirMappingJob

import scala.collection.mutable
import scala.concurrent.Future

trait IJobRepository {

  /**
   * Returns a map of mapping jobs managed by this repository
   *
   * @return
   */
  def getCachedMappingsJobs: mutable.Map[String, mutable.Map[String, FhirMappingJob]]

  /**
   * Retrieve all jobs
   *
   * @param projectId project id the jobs belong to
   * @return
   */
  def getAllJobs(projectId: String): Future[Seq[FhirMappingJob]]

  /**
   * Save the job to the repository.
   *
   * @param projectId project id the job belongs to
   * @param job       job to save
   * @return
   */
  def saveJob(projectId: String, job: FhirMappingJob): Future[FhirMappingJob]

  /**
   * Get the job by its id
   *
   * @param projectId project id the job belongs to
   * @param jobId     job id
   * @return
   */
  def getJob(projectId: String, jobId: String): Future[Option[FhirMappingJob]]

  /**
   * Update the job in the repository
   *
   * @param projectId project id the job belongs to
   * @param jobId     job id
   * @param job       job to save
   * @return
   */
  def updateJob(projectId: String, jobId: String, job: FhirMappingJob): Future[FhirMappingJob]

  /**
   * Delete the job from the repository
   *
   * @param projectId project id the job belongs to
   * @param jobId     job id
   * @return
   */
  def deleteJob(projectId: String, jobId: String): Future[Unit]

  /**
   * Deletes all jobs associated with a specific project.
   *
   * @param projectId The unique identifier of the project for which jobs should be deleted.
   */
  def deleteProjectJobs(projectId: String): Unit

  /**
   * Retrieves the jobs referencing the given mapping in their definitions.
   *
   * @param projectId  identifier of project whose jobs will be checked
   * @param mappingUrl the url of mapping
   * @return the jobs referencing the given mapping in their definitions
   */
  def getJobsReferencingMapping(projectId: String, mappingUrl: String): Future[Seq[FhirMappingJob]]
}
