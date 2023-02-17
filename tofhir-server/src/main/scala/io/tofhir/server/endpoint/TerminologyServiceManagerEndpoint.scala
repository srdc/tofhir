package io.tofhir.server.endpoint

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.config.ToFhirEngineConfig
import io.tofhir.server.endpoint.TerminologyServiceManagerEndpoint._
import io.tofhir.server.model.Json4sSupport._
import io.tofhir.server.model.{TerminologySystem, ToFhirRestCall}
import io.tofhir.server.service.terminology.TerminologySystemFolderRepository

import scala.concurrent.Future

class TerminologyServiceManagerEndpoint(toFhirEngineConfig: ToFhirEngineConfig) extends LazyLogging {

  private val terminologySystemFolderRepository: TerminologySystemFolderRepository = new TerminologySystemFolderRepository(toFhirEngineConfig.contextPath)

  private val conceptMapEndpoint: ConceptMapEndpoint = new ConceptMapEndpoint(toFhirEngineConfig)
  private val codeSystemEndpoint: CodeSystemEndpoint = new CodeSystemEndpoint(toFhirEngineConfig)

  def route(request: ToFhirRestCall): Route = {
    pathPrefix(SEGMENT_TERMINOLOGY) {
      // operations on all terminology systems
      pathEndOrSingleSlash {
        createTerminologySystemRoute() ~ getAllTerminologySystemsRoute
      } ~ // operations on individual terminology systems
        pathPrefix(Segment) { terminologySystemId: String =>
          pathEndOrSingleSlash {
            getTerminologySystemRoute(terminologySystemId) ~ putTerminologySystemRoute(terminologySystemId) ~ deleteTerminologySystemRoute(terminologySystemId)
          } ~ {
            val terminologyExists: Future[Option[TerminologySystem]] = terminologySystemFolderRepository.getTerminologySystem(terminologySystemId)
            onSuccess(terminologyExists) {
              case None => complete {
                StatusCodes.NotFound -> s"TerminologySystem with id $terminologySystemId not found"
              }
              case Some(_) => {
                request.terminologyId = Some(terminologySystemId)
                conceptMapEndpoint.route(request) ~ codeSystemEndpoint.route(request)
              }
            }
          }
        }
    }
  }

  /**
   * Route to get all local TerminologySystems
   *
   * @return
   */
  private def getAllTerminologySystemsRoute: Route = {
    get {
      complete {
        terminologySystemFolderRepository.getTerminologySystemsMetadata
      }
    }
  }

  /**
   * Route to create a local TerminologySystem
   *
   * @return
   */
  private def createTerminologySystemRoute(): Route = {
    post {
      entity(as[TerminologySystem]) { terminologySystem =>
        complete {
          terminologySystemFolderRepository.createTerminologySystem(terminologySystem) map { createdTerminologySystem =>
            StatusCodes.Created -> createdTerminologySystem
          }
        }
      }
    }
  }

  /**
   * Route to get a local TerminologySystem
   *
   * @param terminologySystemId id of local TerminologySystem
   * @return
   */
  private def getTerminologySystemRoute(terminologySystemId: String): Route = {
    get {
      complete {
        terminologySystemFolderRepository.getTerminologySystem(terminologySystemId) map {
          case Some(terminologySystem) => StatusCodes.OK -> terminologySystem
          case None => StatusCodes.NotFound -> s"TerminologySystem with id $terminologySystemId not found."
        }
      }
    }
  }

  /**
   * Route to update a local TerminologySystem
   *
   * @param terminologySystemId id of local TerminologySystem
   * @return
   */
  private def putTerminologySystemRoute(terminologySystemId: String): Route = {
    put {
      entity(as[TerminologySystem]) { newTerminologySystem =>
        complete {
          terminologySystemFolderRepository.updateTerminologySystem(terminologySystemId, newTerminologySystem) map { updatedTerminologySystem =>
            StatusCodes.OK -> updatedTerminologySystem
          }
        }
      }
    }
  }

  /**
   * Route to delete a local TerminologySystem
   *
   * @param terminologySystemId id of local TerminologySystem
   * @return
   */
  private def deleteTerminologySystemRoute(terminologySystemId: String): Route = {
    delete {
      complete {
        terminologySystemFolderRepository.deleteTerminologySystem(terminologySystemId) map {
          _ => StatusCodes.NoContent
        }
      }
    }
  }

}

object TerminologyServiceManagerEndpoint {
  val SEGMENT_TERMINOLOGY = "terminology"
  val SEGMENT_CONTENT = "content"
  val ATTACHMENT = "attachment"
}
