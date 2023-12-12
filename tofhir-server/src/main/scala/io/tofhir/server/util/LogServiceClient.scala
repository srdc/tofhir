package io.tofhir.server.util

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, Uri}
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
   * @param page desired number of page
   * @param dateBefore last date of the filtered executions
   * @param dateAfter start date of the filtered executions
   * @param errorStatuses desired error status of the filtered executions
   * @return A future of a tuple containing
   *         the details of individual executions as the first element
   *         total number of executions as the second element
   *         number of executions after applying filter as the third element
   *         //TODO we can define a dedicated class representing the response type
   */
  def getExecutions(projectId: String, jobId: String, page: Int, dateBefore: String, dateAfter: String, errorStatuses: String ): Future[(Seq[JValue], Long, Long)] = {
    val params = Map("page" -> page.toString,
                     "dateBefore" -> dateBefore,
                     "dateAfter" -> dateAfter,
                     "errorStatuses" -> errorStatuses)
    val uri: Uri = s"$logServiceEndpoint/projects/$projectId/jobs/$jobId/executions"
    val request = HttpRequest(
      method = HttpMethods.GET,
    ).withUri(uri.withQuery(Uri.Query(params)))

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
