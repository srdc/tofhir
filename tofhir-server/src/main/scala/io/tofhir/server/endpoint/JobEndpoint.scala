package io.tofhir.server.endpoint

import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.StreamTcpException
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.model.FhirMappingJob
import io.tofhir.server.endpoint.JobEndpoint.{SEGMENT_EXECUTIONS, SEGMENT_JOB, SEGMENT_MAPPINGS, SEGMENT_RUN, SEGMENT_STOP, SEGMENT_TEST, SEGMENT_LOGS}
import io.tofhir.server.model.Json4sSupport._
import io.tofhir.server.model.{ExecuteJobTask, ResourceNotFound, RowSelectionOrder, TestResourceCreationRequest, ToFhirRestCall}
import io.tofhir.server.service.{ExecutionService, JobService}
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.util.FhirMappingJobFormatter.formats
import io.tofhir.server.interceptor.ICORSHandler
import io.tofhir.server.service.job.IJobRepository
import io.tofhir.server.service.mapping.IMappingRepository
import io.tofhir.server.service.schema.ISchemaRepository

class JobEndpoint(jobRepository: IJobRepository, mappingRepository: IMappingRepository, schemaRepository: ISchemaRepository, logServiceEndpoint: String) extends LazyLogging {

  val service: JobService = new JobService(jobRepository)
  val executionService: ExecutionService = new ExecutionService(jobRepository, mappingRepository, schemaRepository, logServiceEndpoint)

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
        } ~ pathPrefix(SEGMENT_TEST) { // test a mapping with mapping job configurations, jobs/<jobId>/test
          pathEndOrSingleSlash {
            testMappingWithJob(projectId, jobId)
          }
        } ~ pathPrefix(SEGMENT_EXECUTIONS) { // Operations on all executions, jobs/<jobId>/executions
          pathEndOrSingleSlash {
            getExecutions(projectId, jobId)
          } ~ pathPrefix(Segment) { executionId: String => // operations on a single execution, jobs/<jobId>/executions/<executionId>
            pathEndOrSingleSlash {
              getExecutionById(projectId, jobId, executionId)
            } ~ pathPrefix(SEGMENT_LOGS) { // logs on a single execution, jobs/<jobId>/executions/<executionId>/logs
              pathEndOrSingleSlash {
                getExecutionLogs(projectId, jobId, executionId)
              }
            } ~ pathPrefix(SEGMENT_RUN) { // jobs/<jobId>/executions/<executionId>/run
              pathEndOrSingleSlash {
                continueJobExecution(projectId, jobId, executionId)
              }
            } ~ pathPrefix(SEGMENT_STOP) { // jobs/<jobId>/executions/<executionId>/stop
              pathEndOrSingleSlash {
                stopJobExecution(jobId, executionId)
              }
            } ~ pathPrefix(SEGMENT_MAPPINGS) { // jobs/<jobId>/executions/<executionId>/mappings
              pathPrefix(Segment) { mappingUrl: String => // jobs/<jobId>/executions/<executionId>/mappings/<mappingUrl>
                pathPrefix(SEGMENT_STOP) { // jobs/<jobId>/executions/<executionId>/mappings/<mappingUrl>/stop
                  pathEndOrSingleSlash {
                    stopMappingExecution(jobId, executionId, mappingUrl)
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

  private def getJob(projectId: String, id: String): Route = {
    get {
      complete {
        service.getJob(projectId, id) map {
          case Some(mappingJob) => StatusCodes.OK -> mappingJob
          case None => StatusCodes.NotFound -> {
            throw ResourceNotFound("Job not found", s"Mapping job with name $id not found")
          }
        }
      }
    }
  }

  private def updateJob(projectId: String, id: String): Route = {
    put {
      entity(as[FhirMappingJob]) { job =>
        complete {
          service.updateJob(projectId, id, job) map { _ =>
            StatusCodes.OK -> job
          }
        }
      }
    }
  }

  private def deleteJob(projectId: String, id: String): Route = {
    delete {
      complete {
        service.deleteJob(projectId, id) map { _ =>
          StatusCodes.NoContent
        }
      }
    }
  }

  private def runJob(projectId: String, id: String): Route = {
    post {
      entity(as[Option[ExecuteJobTask]]) { executeJobTask =>
        complete {
          executionService.runJob(projectId, id, None, executeJobTask) map { _ =>
            StatusCodes.OK
          }
        }
      }
    }
  }

  /**
   * Route to test a mapping with mapping job configurations i.e. source data configurations
   * */
  private def testMappingWithJob(projectId: String, id: String): Route = {
    post {
      entity(as[TestResourceCreationRequest]) { requestBody =>
        validate(RowSelectionOrder.isValid(requestBody.resourceFilter.order),
          "Invalid row selection order. Available options are: start, end, random") {
          complete {
            executionService.testMappingWithJob(projectId, id, requestBody)
          }
        }
      }
    }
  }

  /**
   * Route to get executions of a mapping job
   * */
  private def getExecutions(projectId: String, id: String): Route = {
    get {
      parameterMap { queryParams => // page is supported for now (e.g. page=1)
        onComplete(executionService.getExecutions(projectId, id, queryParams)) {
          case util.Success(response) =>
            val headers = List(
              RawHeader(ICORSHandler.X_TOTAL_COUNT_HEADER, response._2.toString)
            )
            respondWithHeaders(headers) {
              complete(response._1)
            }
          case util.Failure(exception) =>
            exception match {
              case e:StreamTcpException =>
                logger.error(s"Failed to retrieve executions for project $projectId job $id",e)
                complete {
                  HttpResponse(
                    status = StatusCodes.GatewayTimeout,
                    entity = "The toFHIR Log Server is currently unavailable. Please try again later."
                  )
                }
              case t:Throwable => throw t
            }
        }
      }
    }
  }

  /**
   * Route to get execution logs of a mapping job execution
   * @param projectId
   * @param jobId
   * @param executionId
   * @return
   */
  private def getExecutionById(projectId: String, jobId: String, executionId: String): Route = {
    get {
      complete {
        executionService.getExecutionById(projectId, jobId, executionId)
      }
    }
  }

  /**
   * Route to continue a job execution with parameters (e.g. clearCheckpoint, mappingErrorHandling)
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
   * Route to stop an individual mapping task inside a job.
   *
   * @param jobId      Identifier of the job containing the mapping.
   * @param mappingUrl Url of the mapping to be stopped
   * @return
   */
  private def stopMappingExecution(jobId: String, executionId: String, mappingUrl: String): Route = {
    delete {
      complete {
        executionService.stopMappingExecution(jobId, executionId, mappingUrl).map(_ => StatusCodes.OK)
      }
    }
  }

  /**
   * Route to retrieve execution logs i.e. the logs of mapping task which are ran in the execution
   * */
  private def getExecutionLogs(projectId: String, jobId: String, executionId: String): Route = {
    get {
      complete {
        executionService.getExecutionLogs(projectId: String, jobId: String, executionId: String)
      }
    }
  }
}

object JobEndpoint {
  val SEGMENT_JOB = "jobs"
  val SEGMENT_RUN = "run"
  val SEGMENT_EXECUTIONS = "executions"
  val SEGMENT_LOGS = "logs"
  val SEGMENT_TEST = "test"
  val SEGMENT_STOP = "stop"
  val SEGMENT_MAPPINGS = "mappings"
}
