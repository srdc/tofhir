package io.tofhir.server.endpoint

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives.{complete, entity, _}
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.config.ToFhirEngineConfig
import io.tofhir.server.endpoint.CodeSystemEndpoint.SEGMENT_CODESYSTEM
import io.tofhir.server.endpoint.LocalTerminologyEndpoint._
import io.tofhir.server.model.Json4sSupport._
import io.tofhir.server.model.{TerminologyCodeSystem, ToFhirRestCall}
import io.tofhir.server.service.CodeSystemService

class CodeSystemEndpoint(toFhirEngineConfig: ToFhirEngineConfig) extends LazyLogging {

  val service: CodeSystemService = new CodeSystemService(toFhirEngineConfig.contextPath)

  def route(request: ToFhirRestCall): Route = {
    pathPrefix(SEGMENT_CODESYSTEM) {
      val terminologyId: String = request.terminologyId.get
      pathEndOrSingleSlash {
        getAllCodeSystemsRoute(terminologyId) ~ createCodeSystemRoute(terminologyId)
      } ~ pathPrefix(Segment) { codeSystemId =>
        pathEndOrSingleSlash {
          getCodeSystemRoute(terminologyId, codeSystemId) ~ putCodeSystemRoute(terminologyId, codeSystemId) ~ deleteCodeSystemRoute(terminologyId, codeSystemId)
        } ~ pathPrefix(SEGMENT_CONTENT) {
          pathEndOrSingleSlash {
            uploadDownloadCodeSystemFileRoute(terminologyId, codeSystemId)
          }
        }
      }
    }
  }

  /**
   * Route to get all code systems within a terminology
   *
   * @return
   */
  private def getAllCodeSystemsRoute(terminologyId: String): Route = {
    get {
      complete {
        service.getCodeSystems(terminologyId) map { metadata =>
          StatusCodes.OK -> metadata
        }
      }
    }
  }

  /**
   * Route to create a code system within a terminology
   *
   * @return
   */
  private def createCodeSystemRoute(terminologyId: String): Route = {
    post {
      entity(as[TerminologyCodeSystem]) { codeSystem =>
        complete {
          service.createCodeSystem(terminologyId, codeSystem) map { created =>
            StatusCodes.Created -> created
          }
        }
      }
    }
  }

  /**
   * Route to get a code system terminology
   *
   * @param terminologyId id of code system terminology
   * @param codeSystemId  id of code system
   * @return
   */
  private def getCodeSystemRoute(terminologyId: String, codeSystemId: String): Route = {
    get {
      complete {
        service.getCodeSystem(terminologyId, codeSystemId) map {
          case Some(codeSystem) => StatusCodes.OK -> codeSystem
          case None => StatusCodes.NotFound -> s"Concept map  with id $codeSystemId not found"
        }
      }
    }
  }

  /**
   * Route to put a code system terminology
   *
   * @param terminologyId id of code system terminology
   * @param codeSystemId  id of code system
   * @return
   */
  private def putCodeSystemRoute(terminologyId: String, codeSystemId: String): Route = {
    put {
      entity(as[TerminologyCodeSystem]) { codeSystem =>
        complete {
          service.updateCodeSystem(terminologyId, codeSystemId, codeSystem) map {
            terminologyCodeSystem => StatusCodes.OK -> terminologyCodeSystem
          }
        }
      }
    }
  }

  /**
   * Route to delete a code system terminology
   *
   * @param terminologyId id of code system terminology
   * @param codeSystemId  id of code system
   * @return
   */
  private def deleteCodeSystemRoute(terminologyId: String, codeSystemId: String): Route = {
    delete {
      complete {
        service.removeCodeSystem(terminologyId, codeSystemId) map {
          _ => StatusCodes.NoContent
        }
      }
    }
  }

  /**
   * Route to upload/download a code system file
   *
   * @param terminologyId id of code system terminology
   * @param codeSystemId  id of code system
   * @return
   */
  private def uploadDownloadCodeSystemFileRoute(terminologyId: String, codeSystemId: String): Route = {
    post {
      fileUpload(ATTACHMENT) {
        case (fileInfo, byteSource) =>
          complete {
            service.uploadCodeSystemFile(terminologyId, codeSystemId, byteSource) map {
              _ => StatusCodes.OK
            }
          }
      }
    } ~ get {
      complete {
        service.downloadCodeSystemFile(terminologyId, codeSystemId) map { byteSource =>
          HttpResponse(StatusCodes.OK, entity = HttpEntity(ContentTypes.`text/csv(UTF-8)`, byteSource))
        }
      }
    }
  }

}

object CodeSystemEndpoint {
  val SEGMENT_CODESYSTEM = "codesystem"
}
