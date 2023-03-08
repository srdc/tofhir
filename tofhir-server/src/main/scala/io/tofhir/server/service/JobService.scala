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
  def monitorJob(projectId: String, jobId: String, queryParams: Map[String, String]): Future[Seq[FhirMappingJobLog]] = {
    // retrieve the job to validate its existence
    val page = queryParams.getOrElse("page", "1").toInt
    jobRepository.getJob(projectId, jobId).map {
      case Some(j) =>
        // read logs/tofhir-mappings.log file
        val dataFrame = SparkConfig.sparkSession.read.json("logs/tofhir-mappings.log")
        // TODO: tofhir-mappings.log does not include the project information
        val jobRuns = dataFrame.filter(s"jobId = '$jobId'")
        // handle the case where the job has not been run yet which makes the data frame empty
        if (jobRuns.isEmpty) {
          Seq.empty
        } else {
          // page size is 50, handle pagination
          val total = jobRuns.count()
          val numOfPages = Math.ceil(total.toDouble / 50).toInt
          if (page > numOfPages) {
            throw ResourceNotFound("Page does not exist.", s"Page $page does not exist")
          }
          val start = (page - 1) * 50
          val end = Math.min(start + 50, total.toInt)
          // sort the runs by latest to oldest
          val paginatedRuns = jobRuns.sort(jobRuns.col("@timestamp").desc).collect().slice(start, end)
          // create the log model for each job run
          paginatedRuns.map(row => {
            // get mapping url
            val mappingUrl = row.getAs[String]("mappingUrl") match {
              case null => None
              case url => Some(url)
            }
            // return the log
            FhirMappingJobLog(jobId = row.getAs[String]("jobId"),
              mappingUrl = mappingUrl,
              numOfInvalids = row.getAs[Long]("numOfInvalids"),
              numOfNotMapped = row.getAs[Long]("numOfNotMapped"),
              numOfFhirResources = row.getAs[Long]("numOfFhirResources"),
              numOfFailedWrites = row.getAs[Long]("numOfFailedWrites"),
              timestamp = row.getAs[String]("@timestamp"),
              result = row.getAs[String]("result"),
              message = row.getAs[String]("message"))
          })
        }
      case None => throw ResourceNotFound("Mapping job does not exists.", s"A mapping job with id $jobId does not exists")
    }
  }
}
