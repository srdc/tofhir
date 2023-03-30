package io.tofhir.server.service.job

import io.tofhir.engine.model.{FhirMappingJob, FhirMappingResult, FhirMappingTask}

import scala.concurrent.Future

trait IJobRepository {

  /**
   * Retrieve all jobs
   * @param projectId project id the jobs belong to
   * @return
   */
  def getAllJobs(projectId: String): Future[Seq[FhirMappingJob]]

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

  /**
   * Run the job for the specified mapping tasks. If no mapping tasks are specified, run the mapping job for all
   * of them.
   * @param projectId project id the job belongs to
   * @param id job id
   * @param mappingUrls the urls of mapping tasks to be executed
   * @return
   */
  def runJob(projectId: String, id: String, mappingUrls: Option[Seq[String]]=None): Future[Future[Unit]]

  /**
   * Tests the given mapping task by running it with mapping job configurations (i.e. source data configurations) and
   * returns its results
   *
   * @param projectId project id the job belongs to
   * @param id job id
   * @param mappingTask mapping task to be executed
   * @return
   */
  def testMappingWithJob(projectId: String, id: String, mappingTask: FhirMappingTask): Future[Future[Seq[FhirMappingResult]]]
}
