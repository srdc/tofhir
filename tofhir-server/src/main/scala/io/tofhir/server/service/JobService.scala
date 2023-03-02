package io.tofhir.server.service

import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.model.FhirMappingJob
import io.tofhir.server.config.SparkConfig
import io.tofhir.server.model.{FhirMappingJobLog, ResourceNotFound}
import io.tofhir.server.service.job.IJobRepository
import org.apache.spark.sql.Row

import scala.concurrent.ExecutionContext.Implicits.global
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

  /**
   * Run the job
   * @param projectId project id the job belongs to
   * @param jobId job id
   * @return
   */
  def runJob(projectId: String, jobId: String): Future[Unit] = {
    jobRepository.runJob(projectId, jobId)
  }

  /**
   * Monitors the result of a mapping job. It extracts the log information from {@link logs/tofhir-mappings.log}
   * file for the given mapping job and returns the last log.
   *
   * @param projectId project id the job belongs to
   * @param jobId     job id
   * @return the result of mapping job. It returns {@link None} if the job has not been run before.
   * @throws ResourceNotFound when mapping job does not exist
   */
  def monitorJob(projectId: String, jobId: String): Future[Option[FhirMappingJobLog]] = {
    // retrieve the job to validate its existence
    val job = jobRepository.getJob(projectId, jobId)
    job
      .recover(_ => None) // catch the exception indicating that mapping job does not exist and return None
      .map[Option[FhirMappingJobLog]](job => {
        // throw an exception if the mapping job does not exist
        if (job.isEmpty) {
          throw ResourceNotFound("Mapping job does not exists.", s"A mapping job with id $jobId does not exists")
        }
        // read logs/tofhir-mappings.log file
        val dataFrame = SparkConfig.sparkSession.read.json("logs/tofhir-mappings.log")
        // find the log corresponding to the last run of mapping job
        var lastRunOfMappingJob: Array[Row] = Array()
        // handle the case where no job has been run yet which makes the data frame empty
        if (!dataFrame.isEmpty) {
          // filter logs by mapping job id
          val x = dataFrame.filter(s"jobId = '$jobId'") // TODO: tofhir-mappings.log does not include the project information
          lastRunOfMappingJob = x.tail(1)
        }

        if (lastRunOfMappingJob.length == 0) {
          // job has not been run before
          None
        } else {
          val row = lastRunOfMappingJob.head
          // get mapping url
          val mappingUrl = row.getAs[String]("mappingUrl") match {
            case null => None
            case url => Some(url)
          }
          // return the log
          Some(FhirMappingJobLog(jobId = row.getAs[String]("jobId"),
            mappingUrl = mappingUrl,
            numOfInvalids = row.getAs[Long]("numOfInvalids"),
            numOfNotMapped = row.getAs[Long]("numOfNotMapped"),
            numOfFhirResources = row.getAs[Long]("numOfFhirResources"),
            numOfFailedWrites = row.getAs[Long]("numOfFailedWrites"),
            timestamp = row.getAs[String]("@timestamp"),
            result = row.getAs[String]("result"),
            message = row.getAs[String]("message")))
        }
      })
  }
}
