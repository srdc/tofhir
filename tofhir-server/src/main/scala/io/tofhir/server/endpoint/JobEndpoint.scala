package io.tofhir.server.endpoint

import akka.http.scaladsl.model.{HttpEntity, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.model.FhirMappingJob
import io.tofhir.server.endpoint.JobEndpoint.{SEGMENT_DESCHEDULE, SEGMENT_EXECUTIONS, SEGMENT_JOB, SEGMENT_MAPPINGS, SEGMENT_RUN, SEGMENT_STATUS, SEGMENT_STOP, SEGMENT_TEST}
import io.onfhir.definitions.common.model.Json4sSupport._
import io.tofhir.server.model.{ExecuteJobTask, RowSelectionOrder, TestResourceCreationRequest}
import io.tofhir.server.service.{ExecutionService, JobService}
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.util.FhirMappingJobFormatter.formats
import io.tofhir.server.common.model.{ResourceNotFound, ToFhirRestCall}
import io.tofhir.server.repository.job.IJobRepository
import io.tofhir.server.repository.mapping.IMappingRepository
import io.tofhir.server.repository.schema.ISchemaRepository
import io.tofhir.server.common.interceptor.ICORSHandler
import scala.concurrent.Future

class JobEndpoint(jobRepository: IJobRepository, mappingRepository: IMappingRepository, schemaRepository: ISchemaRepository) extends LazyLogging with ICORSHandler {

  val service: JobService = new JobService(jobRepository)
  val executionService: ExecutionService = new ExecutionService(jobRepository, mappingRepository, schemaRepository)

  def route(request: ToFhirRestCall): Route = {
    pathPrefix(SEGMENT_JOB) {
      val projectId: String = request.projectId.get
      pathEndOrSingleSlash { // Operations on all mapping jobs
        getAllJobs(projectId) ~ createJob(projectId)
      } ~ pathPrefix(Segment) { jobId: String =>
        pathEndOrSingleSlash { // Operations on a single job, jobs/<jobId>
          getJob(projectId, jobId) ~ updateJob(projectId, jobId) ~ deleteJob(projectId, jobId)
        } ~ pathPrefix(SEGMENT_RUN) { // run a mapping job, jobs/<jobId>/run
          pathEndOrSingleSlash {
            runJob(projectId, jobId)
          }
        } ~ pathPrefix(SEGMENT_STATUS) { // check whether a mapping job is running, jobs/<jobId>/status
          pathEndOrSingleSlash {
            isJobRunning(jobId)
          }
        } ~ pathPrefix(SEGMENT_TEST) { // test a mapping with mapping job configurations, jobs/<jobId>/test
          pathEndOrSingleSlash {
            testMappingWithJob(projectId, jobId)
          }
        } ~ pathPrefix(SEGMENT_EXECUTIONS) { // Operations on all executions, jobs/<jobId>/executions
          pathEndOrSingleSlash {
            getExecutions(projectId, jobId) ~ stopExecutions(jobId)
          } ~ pathPrefix(Segment) { executionId: String => // operations on a single execution, jobs/<jobId>/executions/<executionId>
            pathPrefix(SEGMENT_RUN) { // jobs/<jobId>/executions/<executionId>/run
              pathEndOrSingleSlash {
                continueJobExecution(projectId, jobId, executionId)
              }
            } ~ pathPrefix(SEGMENT_STOP) { // jobs/<jobId>/executions/<executionId>/stop
              pathEndOrSingleSlash {
                stopJobExecution(jobId, executionId)
              }
            } ~ pathPrefix(SEGMENT_DESCHEDULE) { // jobs/<jobId>/executions/<executionId>/deschedule
              pathEndOrSingleSlash {
                descheduleJobExecution(jobId, executionId)
              }
            } ~ pathPrefix(SEGMENT_MAPPINGS) { // jobs/<jobId>/executions/<executionId>/mappings
              pathPrefix(Segment) { mappingTaskName: String => // jobs/<jobId>/executions/<executionId>/mappings/<mappingTaskName>
                pathPrefix(SEGMENT_STOP) { // jobs/<jobId>/executions/<executionId>/mappings/<mappingTaskName>/stop
                  pathEndOrSingleSlash {
                    stopMappingExecution(jobId, executionId, mappingTaskName)
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  private def getAllJobs(projectId: String): Route = {
    get {
      complete {
        service.getAllMetadata(projectId)
      }
    }
  }

  private def createJob(projectId: String): Route = {
    post {
      entity(as[FhirMappingJob]) { job =>
        complete {
          service.createJob(projectId, job) map { created =>
            StatusCodes.Created -> created
          }
        }
      }
    }
  }

  private def getJob(projectId: String, jobId: String): Route = {
    get {
      complete {
        service.getJob(projectId, jobId) map {
          case Some(mappingJob) => StatusCodes.OK -> mappingJob
          case None => StatusCodes.NotFound -> {
            throw ResourceNotFound("Job not found", s"Mapping job with name $jobId not found")
          }
        }
      }
    }
  }

  private def updateJob(projectId: String, jobId: String): Route = {
    put {
      entity(as[FhirMappingJob]) { job =>
        complete {
          service.updateJob(projectId, jobId, job) map { _ =>
            StatusCodes.OK -> job
          }
        }
      }
    }
  }

  /**
   * Route to delete a mapping job if it is not currently running.
   *
   * If the job is running, a BadRequest response is returned, indicating that running mapping jobs cannot
   * be deleted. If the job is not running, the job is deleted.
   *
   * @param projectId The identifier of the project to which the mapping job belongs.
   * @param jobId     The identifier of the mapping job to be deleted.
   */
  private def deleteJob(projectId: String, jobId: String): Route = {
    delete {
      complete {
        executionService.isJobRunning(jobId) flatMap { result =>
          if (result)
            Future {
              StatusCodes.BadRequest -> s"The running mapping jobs cannot be deleted."
            }
          else
            service.deleteJob(projectId, jobId) map { _ =>
              StatusCodes.NoContent -> HttpEntity.Empty
            }
        }
      }
    }
  }

  private def runJob(projectId: String, jobId: String): Route = {
    post {
      entity(as[Option[ExecuteJobTask]]) { executeJobTask =>
        complete {
          executionService.runJob(projectId, jobId, None, executeJobTask) map { _ =>
            StatusCodes.OK
          }
        }
      }
    }
  }

  /**
   * Route to check if a mapping job with the specified ID is currently running.
   *
   * @param jobId The identifier of the mapping job to be checked for running status.
   */
  private def isJobRunning(jobId: String): Route = {
    get {
      complete {
        executionService.isJobRunning(jobId).map(result => result.toString)
      }
    }
  }

  /**
   * Route to test a mapping with mapping job configurations i.e. source data configurations
   * */
  private def testMappingWithJob(projectId: String, jobId: String): Route = {
    post {
      entity(as[TestResourceCreationRequest]) { requestBody =>
        validate(RowSelectionOrder.isValid(requestBody.resourceFilter.order),
          s"Invalid row selection order. Available options are: ${RowSelectionOrder.START}, ${RowSelectionOrder.RANDOM}") {
          complete {
            executionService.testMappingWithJob(projectId, jobId, requestBody)
          }
        }
      }
    }
  }

  /**
   * Route to get executions of a mapping job
   * */
  private def getExecutions(projectId: String, jobId: String): Route = {
    get {
      complete {
        executionService.getExecutions(projectId, jobId)
      }
    }
  }

  /**
   * Route to handle the stopping of executions for a mapping job.
   *
   * @param jobId The identifier of the mapping job for which executions should be stopped.
   */
  private def stopExecutions(jobId: String): Route = {
    delete {
      complete {
        executionService.stopJobExecutions(jobId)
      }
    }
  }

  /**
   * Route to continue a job execution with parameters (e.g. clearCheckpoint)
   *
   * @param projectId
   * @param jobId
   * @param executionId
   * @return
   */
  private def continueJobExecution(projectId: String, jobId: String, executionId: String): Route = {
    post {
      entity(as[Option[ExecuteJobTask]]) { executeJobTask =>
        complete {
          executionService.runJob(projectId, jobId, Some(executionId), executeJobTask) map { _ =>
            StatusCodes.OK
          }
        }
      }
    }
  }

  /**
   * Route to stop a job (i.e. all the mappings included inside a job)
   *
   * @param jobId Identifier of the job
   * @return
   */
  private def stopJobExecution(jobId: String, executionId: String): Route = {
    delete {
      complete {
        executionService.stopJobExecution(jobId, executionId).map(_ => StatusCodes.OK)
      }
    }
  }

  /**
   * Route to deschedule a mapping job execution.
   *
   * @param jobId       Identifier of the job
   * @param executionId Identifier of job execution
   * @return
   */
  private def descheduleJobExecution(jobId: String, executionId: String): Route = {
    delete {
      complete {
        executionService.descheduleJobExecution(jobId, executionId).map(_ => StatusCodes.OK)
      }
    }
  }

  /**
   * Route to stop an individual mapping task inside a job.
   *
   * @param jobId           Identifier of the job containing the mapping.
   * @param mappingTaskName Name of the mappingTask to be stopped
   * @return
   */
  private def stopMappingExecution(jobId: String, executionId: String, mappingTaskName: String): Route = {
    delete {
      complete {
        executionService.stopMappingExecution(jobId, executionId, mappingTaskName).map(_ => StatusCodes.OK)
      }
    }
  }
}

object JobEndpoint {
  val SEGMENT_JOB = "jobs"
  val SEGMENT_RUN = "run"
  val SEGMENT_STATUS = "status"
  val SEGMENT_EXECUTIONS = "executions"
  val SEGMENT_TEST = "test"
  val SEGMENT_STOP = "stop"
  val SEGMENT_DESCHEDULE = "deschedule"
  val SEGMENT_MAPPINGS = "mappings"
}
