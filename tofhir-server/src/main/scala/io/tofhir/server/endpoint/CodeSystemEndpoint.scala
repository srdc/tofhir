package io.tofhir.server.endpoint

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.server.endpoint.CodeSystemEndpoint.SEGMENT_CODE_SYSTEMS
import io.tofhir.server.endpoint.TerminologyServiceManagerEndpoint._
import io.tofhir.server.model.Json4sSupport._
import io.tofhir.server.model.{ResourceNotFound, ToFhirRestCall}
import io.tofhir.server.service.CodeSystemService
import io.tofhir.server.service.terminology.codesystem.ICodeSystemRepository

class CodeSystemEndpoint(codeSystemRepository: ICodeSystemRepository) extends LazyLogging {

  val service: CodeSystemService = new CodeSystemService(codeSystemRepository)

  def route(request: ToFhirRestCall): Route = {
    pathPrefix(SEGMENT_CODE_SYSTEMS) {
      val terminologyId: String = request.terminologyId.get
      pathEndOrSingleSlash {
        getAllCodeSystemsRoute(terminologyId)
      } ~ pathPrefix(Segment) { codeSystemId =>
        pathEndOrSingleSlash {
          getCodeSystemRoute(terminologyId, codeSystemId)
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
          case None => StatusCodes.NotFound -> {
            throw ResourceNotFound("Code system not found", s"Code system with id $codeSystemId not found")
          }
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
  val SEGMENT_CODE_SYSTEMS = "code-systems"
}
