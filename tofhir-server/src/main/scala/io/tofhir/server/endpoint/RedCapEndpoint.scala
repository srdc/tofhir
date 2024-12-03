package io.tofhir.server.endpoint

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives.{path, _}
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.server.common.model.ToFhirRestCall
import io.tofhir.server.config.RedCapServiceConfig
import io.tofhir.server.endpoint.RedCapEndpoint.{PARAMETER_RELOAD, SEGMENT_NOTIFICATION, SEGMENT_REDCAP}
import io.onfhir.definitions.common.model.Json4sSupport._
import io.tofhir.server.model.redcap.RedCapProjectConfig
import io.tofhir.server.service.RedCapService

/**
 * This class defines routes for handling requests related to tofhir-redcap service.
 *
 * @param redCapServiceConfig The configuration object for the tofhir-redcap service.
 */
class RedCapEndpoint(redCapServiceConfig: RedCapServiceConfig) extends LazyLogging {

  // Create an instance of RedCapService using the provided configuration
  val service: RedCapService = new RedCapService(redCapServiceConfig: RedCapServiceConfig)

  /**
   * Defines the routes for handling requests to the tofhir-redcap service.
   *
   * @param request The incoming REST call request.
   * @return A Route representing the defined routes for the RedCap endpoint.
   */
  def route(request: ToFhirRestCall): Route = {
    pathPrefix(SEGMENT_REDCAP) {
      pathEndOrSingleSlash { // Operations on Redcap projects
        getProjects ~ saveProjects
      } ~ path(Segment) { projectId =>
        deleteRedCapData(projectId)
      } ~ pathPrefix(SEGMENT_NOTIFICATION) {
        pathEndOrSingleSlash { // Operations on Redcap notification url
          getNotificationUrl
        }
      }
    }
  }

  /**
   * Retrieves the notification URL.
   *
   * @return A Route representing the route to retrieve the notification URL.
   */
  private def getNotificationUrl: Route = {
    get {
      complete {
        service.getNotificationUrl.map { notificationUrl =>
          HttpResponse(StatusCodes.OK, entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, notificationUrl))
        }
      }
    }
  }

  /**
   * Saves RedCap projects.
   *
   * @return A Route representing the route to save RedCap projects.
   */
  private def saveProjects: Route = {
    post {
      entity(as[Seq[RedCapProjectConfig]]) { projects =>
        complete {
          service.saveProjects(projects)
        }
      }
    }
  }

  /**
   * Retrieves RedCap projects.
   *
   * @return A Route representing the route to retrieve RedCap projects.
   */
  private def getProjects: Route = {
    get {
      complete {
        service.getProjects
      }
    }
  }

  /**
   * Route to delete REDCap data from Kafka topics.
   *
   * @param projectId The identifier of the project whose data will be deleted.
   * */
  private def deleteRedCapData(projectId: String): Route = {
    delete {
      parameter(PARAMETER_RELOAD) { reload =>
        complete {
          service.deleteRedCapData(projectId = projectId, reload = reload.contentEquals("true"))
        }
      }
    }
  }
}

/**
 * Contains constants for segment names used in the RedCap endpoint.
 */
object RedCapEndpoint {
  val SEGMENT_REDCAP = "redcap"
  val SEGMENT_NOTIFICATION = "notification"
  val PARAMETER_RELOAD = "reload"
}
