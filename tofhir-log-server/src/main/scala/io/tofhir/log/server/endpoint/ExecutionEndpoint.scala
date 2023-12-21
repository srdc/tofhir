package io.tofhir.log.server.endpoint

import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import io.tofhir.log.server.model.Json4sSupport._
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.server.common.interceptor.IErrorHandler
import io.tofhir.log.server.service.ExecutionService
import ExecutionEndpoint.{SEGMENT_EXECUTIONS, SEGMENT_JOB, SEGMENT_LOGS, SEGMENT_PROJECTS}
import io.tofhir.server.common.config.WebServerConfig
import io.tofhir.server.common.interceptor.ICORSHandler

class ExecutionEndpoint(webServerConfig: WebServerConfig) extends ICORSHandler with IErrorHandler with LazyLogging {

  val executionService: ExecutionService = new ExecutionService()

  lazy val toFHIRRoute: Route =
    pathPrefix(webServerConfig.baseUri) {
      corsHandler {
        pathPrefix(SEGMENT_PROJECTS) {
          pathPrefix(Segment) { projectId: String => {
            pathPrefix(SEGMENT_JOB) {
              pathPrefix(Segment) { jobId: String =>
                pathPrefix(SEGMENT_EXECUTIONS) {
                  pathEndOrSingleSlash {
                    getExecutions(projectId, jobId) // Get all executions for this job, jobs/<jobId>/executions
                  } ~ pathPrefix(Segment) { executionId: String =>
                    pathEndOrSingleSlash {
                      getExecutionById(projectId, jobId, executionId) // get an execution, jobs/<jobId>/executions/<executionId>
                    } ~ pathPrefix(SEGMENT_LOGS) {
                      pathEndOrSingleSlash {
                        getExecutionLogs(executionId) // get logs of an execution, jobs/<jobId>/executions/<executionId>/logs
                      }
                    }
                  }
                }
              }
            }
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
      parameterMap { queryParams => // page and filter information is included (Ex: page=1&errorStatus=SUCCESS,FAILURE)
        onComplete(executionService.getExecutions(projectId, id, queryParams)) {
          case util.Success(response) =>
            val headers = List(
              RawHeader(ICORSHandler.X_TOTAL_COUNT_HEADER, response._2.toString)
            )
            respondWithHeaders(headers) {
              complete(response._1)
            }
        }
      }
    }
  }

  /**
   * Route to get execution logs of a mapping job execution
   *
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
   * Route to retrieve execution logs i.e. the logs of mapping task which are ran in the execution
   * */
  private def getExecutionLogs(id: String): Route = {
    get {
      complete {
        executionService.getExecutionLogs(id)
      }
    }
  }
}

object ExecutionEndpoint {
  val SEGMENT_PROJECTS = "projects"
  val SEGMENT_JOB = "jobs"
  val SEGMENT_EXECUTIONS = "executions"
  val SEGMENT_LOGS = "logs"
}
