package io.tofhir.server.util

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpMethods, HttpRequest}
import io.tofhir.engine.Execution.actorSystem
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.server.interceptor.ICORSHandler
import io.tofhir.server.model.Json4sSupport.formats
import org.json4s.JValue
import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

/**
 * A client to connect to the log service
 * // TODO handle exceptional cases in the responses
 *
 * @param logServiceEndpoint
 */
class LogServiceClient(logServiceEndpoint: String) {
  val timeout = 20.seconds

  /**
   * Retrieves logs for the executions associated to a specific job
   *
   * @param projectId
   * @param jobId
   * @param page
   * @param filters start end, end date, error status and running status information to filter executions. Uri is formatted in frontend.
   * @return A future of a tuple containing the details of individual executions and total number of executions
   *         //TODO we can define a dedicated class representing the response type
   */
  def getExecutions(projectId: String, jobId: String, page: Int, filters: String): Future[(Seq[JValue], Long, Long)] = {
    val request = HttpRequest(
      method = HttpMethods.GET,
      uri = s"$logServiceEndpoint/projects/$projectId/jobs/$jobId/executions?page=$page&${filters}"
    )

    var countHeader: Long = 0
    var filteredCountHeader: Long = 0
    Http().singleRequest(request)
      .flatMap(resp => {
        countHeader = resp.headers.find(_.name == ICORSHandler.X_TOTAL_COUNT_HEADER).map(_.value).get.toInt
        filteredCountHeader = resp.headers.find(_.name == ICORSHandler.X_FILTERED_COUNT_HEADER).map(_.value).get.toLong
        resp.entity.toStrict(timeout)
      })
      .map(strictEntity => {
        val response = strictEntity.data.utf8String
        (JsonMethods.parse(response).extract[Seq[JValue]], countHeader, filteredCountHeader)
      })
  }

  /**
   * Retrieves logs for a specific execution
   *
   * @param projectId
   * @param jobId
   * @param executionId
   * @return
   */
  def getExecutionLogs(projectId: String, jobId: String, executionId: String): Future[Seq[JValue]] = {
    val request = HttpRequest(
      method = HttpMethods.GET,
      uri = s"$logServiceEndpoint/projects/$projectId/jobs/$jobId/executions/$executionId/logs"
    )

    Http().singleRequest(request)
      .flatMap(resp => {
        resp.entity.toStrict(timeout)
      })
      .map(strictEntity => {
        val response = strictEntity.data.utf8String
        JsonMethods.parse(response).extract[Seq[JValue]]
      })
  }

  /**
   * Retrieves execution details including mapping tasks, error status, start time, etc.
   *
   * @param projectId
   * @param jobId
   * @param executionId
   * @return
   */
  def getExecutionById(projectId: String, jobId: String, executionId: String): Future[JObject] = {
    val request = HttpRequest(
      method = HttpMethods.GET,
      uri = s"$logServiceEndpoint/projects/$projectId/jobs/$jobId/executions/$executionId"
    )

    Http().singleRequest(request)
      .flatMap { resp => resp.entity.toStrict(timeout) }
      .map(strictEntity => {
        println()
        val response = strictEntity.data.utf8String
        JsonMethods.parse(response).extract[JObject]
      })

  }
}
