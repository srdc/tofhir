package io.tofhir.server.service

import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.model.FhirMappingJob
import io.tofhir.server.config.SparkConfig
import io.tofhir.server.model.ResourceNotFound
import io.tofhir.server.service.job.IJobRepository
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods

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
  def runJob(projectId: String, jobId: String): Future[Future[Unit]] = {
    jobRepository.runJob(projectId, jobId)
  }

  /**
   * Monitors the result of a mapping job. It extracts the logs from {@link logs/tofhir-mappings.log} file for the
   * given mapping job and returns the ones satisfying given parameters (e.g. paging).
   *
   * @param projectId project id the job belongs to
   * @param jobId     job id
   * @param queryParams parameters to filter results such as paging
   * @return the execution logs of mapping job as a JSON array. It returns an empty array if the job has not been run before.
   * @throws ResourceNotFound when mapping job does not exist
   */
  def monitorJob(projectId: String, jobId: String, queryParams: Map[String, String]): Future[Seq[JValue]] = {
    // retrieve the job to validate its existence
    jobRepository.getJob(projectId, jobId).map {
      case Some(_) =>
        // read logs/tofhir-mappings.log file
        val dataFrame = SparkConfig.sparkSession.read.json("logs/tofhir-mappings.log")
        // handle the case where no job has been run yet which makes the data frame empty
        if(dataFrame.isEmpty){
          Seq.empty
        }
        else {
          // TODO: tofhir-mappings.log does not include the project information
          val jobRuns = dataFrame.filter(s"jobId = '$jobId'")
          // handle the case where the job has not been run yet which makes the data frame empty
          if (jobRuns.isEmpty) {
            Seq.empty
          } else {
            // page size is 50, handle pagination
            val total = jobRuns.count()
            val numOfPages = Math.ceil(total.toDouble / 50).toInt
            val page = queryParams.getOrElse("page", "1").toInt
            // handle the case where requested page does not exist
            if (page > numOfPages) {
              Seq.empty
            } else {
              val start = (page - 1) * 50
              val end = Math.min(start + 50, total.toInt)
              // sort the runs by latest to oldest
              val paginatedRuns = jobRuns.sort(jobRuns.col("@timestamp").desc).collect().slice(start, end)
              paginatedRuns.map(row => {
                JsonMethods.parse(row.json)
              })
            }
          }
        }
      case None => throw ResourceNotFound("Mapping job does not exists.", s"A mapping job with id $jobId does not exists")
    }
  }
}
