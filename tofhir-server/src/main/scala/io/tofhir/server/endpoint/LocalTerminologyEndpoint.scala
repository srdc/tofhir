package io.tofhir.server.endpoint

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.{complete, _}
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.config.ToFhirEngineConfig
import io.tofhir.server.endpoint.LocalTerminologyEndpoint._
import io.tofhir.server.model.Json4sSupport._
import io.tofhir.server.model.{LocalTerminology, ToFhirRestCall}
import io.tofhir.server.service.LocalTerminologyService

import scala.concurrent.Future

class LocalTerminologyEndpoint(toFhirEngineConfig: ToFhirEngineConfig) extends LazyLogging {

  val localTerminologyService: LocalTerminologyService = new LocalTerminologyService(toFhirEngineConfig.contextPath)

  val conceptMapEndpoint: ConceptMapEndpoint = new ConceptMapEndpoint(toFhirEngineConfig)
  val codeSystemEndpoint: CodeSystemEndpoint = new CodeSystemEndpoint(toFhirEngineConfig)

  def route(request: ToFhirRestCall): Route = {
    pathPrefix(SEGMENT_TERMINOLOGY) {
      // operations on all terminologies
      pathEndOrSingleSlash {
        createLocalTerminologyRoute() ~ getAllLocalTerminologiesRoute
      } ~ // operations on individual terminologies
        pathPrefix(Segment) { terminologyId: String =>
          pathEndOrSingleSlash {
            getLocalTerminologyRoute(terminologyId) ~ putLocalTerminologyRoute(terminologyId) ~ deleteLocalTerminologyRoute(terminologyId)
          } ~ {
            val terminologyExists: Future[Option[LocalTerminology]] = localTerminologyService.getTerminologyServiceById(terminologyId)
            onSuccess(terminologyExists) {
              case None => complete {
                StatusCodes.NotFound -> s"Terminology with id $terminologyId not found"
              }
              case Some(_) => {
                request.terminologyId = Some(terminologyId)
                conceptMapEndpoint.route(request) ~ codeSystemEndpoint.route(request)
              }
            }
          }
        }
    }
  }

  /**
   * Route to get all local terminology
   *
   * @return
   */
  private def getAllLocalTerminologiesRoute: Route = {
    get {
      complete {
        localTerminologyService.getAllMetadata
      }
    }
  }

  /**
   * Route to create a local terminology server
   *
   * @return
   */
  private def createLocalTerminologyRoute(): Route = {
    post {
      entity(as[LocalTerminology]) { localTerminology =>
        complete {
          localTerminologyService.createTerminologyService(localTerminology) map { createdTerminology =>
            StatusCodes.Created -> createdTerminology
          }
        }
      }
    }
  }

  /**
   * Route to get a local terminology
   *
   * @param terminologyId id of local terminology
   * @return
   */
  private def getLocalTerminologyRoute(terminologyId: String): Route = {
    get {
      complete {
        localTerminologyService.getTerminologyServiceById(terminologyId) map {
          case Some(terminology) => StatusCodes.OK -> terminology
          case None => StatusCodes.NotFound -> s"Terminology with name $terminologyId not found"
        }
      }
    }
  }

  /**
   * Route to put a local terminology
   *
   * @param terminologyId id of local terminology
   * @return
   */
  private def putLocalTerminologyRoute(terminologyId: String): Route = {
    put {
      entity(as[LocalTerminology]) { terminology =>
        complete {
          localTerminologyService.updateTerminologyServiceById(terminologyId, terminology) map {
            terminology => StatusCodes.OK -> terminology
          }
        }
      }
    }
  }

  /**
   * Route to delete a local terminology
   *
   * @param terminologyId id of local terminology
   * @return
   */
  private def deleteLocalTerminologyRoute(terminologyId: String): Route = {
    delete {
      complete {
        localTerminologyService.removeTerminologyServiceById(terminologyId) map {
          _ => StatusCodes.NoContent
        }
      }
    }
  }

}

object LocalTerminologyEndpoint {
  val SEGMENT_TERMINOLOGY = "terminology"
  val SEGMENT_CONTENT = "content"
  val ATTACHMENT = "attachment"
}
