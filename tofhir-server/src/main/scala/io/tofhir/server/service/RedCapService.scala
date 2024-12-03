package io.tofhir.server.service

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpRequest}
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.Execution.actorSystem
import io.tofhir.engine.util.FhirMappingJobFormatter.formats
import io.tofhir.server.config.RedCapServiceConfig
import io.tofhir.server.model.redcap.RedCapProjectConfig
import org.json4s.DefaultFormats
import org.json4s.jackson.{JsonMethods, Serialization}

import io.tofhir.engine.Execution.actorSystem.dispatcher
import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, FiniteDuration}

/**
 * Class for interacting with the tofhir-redcap service.
 *
 * This class provides methods to retrieve notification URLs, save projects, and get projects from the tofhir-redcap service.
 *
 * @param redCapServiceConfig The configuration object for the tofhir-redcap service.
 */
class RedCapService(redCapServiceConfig: RedCapServiceConfig) extends LazyLogging {

  // Timeout duration for proxy requests to the tofhir-redcap service
  private val timeout: FiniteDuration = 20.seconds

  /**
   * Retrieves the notification URL from the tofhir-redcap service.
   *
   * @return A Future containing the notification URL as a String.
   */
  def getNotificationUrl: Future[String] = {
    val proxiedRequest = HttpRequest(
      method = HttpMethods.GET,
      uri = s"${redCapServiceConfig.notificationEndpoint}",
      headers = RawHeader("Content-Type", "application/json") :: Nil
    )

    // Add timeout and transform response to a String
    Http().singleRequest(proxiedRequest)
      .flatMap { resp => resp.entity.toStrict(timeout) }
      .map(strictEntity => strictEntity.data.utf8String)
  }

  /**
   * Saves the provided RedCap project configurations to the tofhir-redcap service.
   *
   * @param requestBody The sequence of RedCapProjectConfig objects to be saved.
   * @return A Future representing the completion of the save operation.
   */
  def saveProjects(requestBody: Seq[RedCapProjectConfig]): Future[Unit] = {
    implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

    val proxiedRequest = HttpRequest(
      method = HttpMethods.POST,
      uri = s"${redCapServiceConfig.projectsEndpoint}",
      headers = RawHeader("Content-Type", "application/json") :: Nil,
      entity = HttpEntity(ContentTypes.`application/json`, Serialization.write(requestBody))
    )

    // Add timeout
    Http().singleRequest(proxiedRequest)
      .flatMap { resp => resp.entity.toStrict(timeout) }
      .map(_ => ())
  }

  /**
   * Retrieves the projects from the tofhir-redcap service.
   *
   * @return A Future containing the sequence of RedCapProjectConfig objects retrieved from the service.
   */
  def getProjects: Future[Seq[RedCapProjectConfig]] = {
    val proxiedRequest = HttpRequest(
      method = HttpMethods.GET,
      uri = s"${redCapServiceConfig.projectsEndpoint}",
      headers = RawHeader("Content-Type", "application/json") :: Nil
    )

    // Add timeout and transform response to a sequence of RedCapProjectConfig objects
    Http().singleRequest(proxiedRequest)
      .flatMap { resp => resp.entity.toStrict(timeout) }
      .map(strictEntity => JsonMethods.parse(strictEntity.data.utf8String).extract[Seq[RedCapProjectConfig]])
  }

  /**
   * Deletes the REDCap data of the project from Kafka topics via tofhir-redcap service.
   *
   * @param projectId The identifier of project
   * @param reload    Whether to reload REDCap data upon the deletion of Kafka topics
   * */
  def deleteRedCapData(projectId: String, reload: Boolean): Future[Unit] = {
    val proxiedRequest = HttpRequest(
      method = HttpMethods.DELETE,
      uri = s"${redCapServiceConfig.projectsEndpoint}/${redCapServiceConfig.projectDataPath}/$projectId?${redCapServiceConfig.projectDataReloadParameter}=$reload",
      headers = RawHeader("Content-Type", "application/json") :: Nil
    )

    // Add timeout
    Http().singleRequest(proxiedRequest)
      .flatMap { resp => resp.entity.toStrict(timeout) }
      .map(_ => ())
  }
}
