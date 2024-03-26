package io.tofhir.server.endpoint

import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.server.common.model.{ResourceNotFound, ToFhirRestCall}
import io.tofhir.server.endpoint.ConceptMapEndpoint.SEGMENT_CONCEPT_MAPS
import io.tofhir.server.endpoint.TerminologyServiceManagerEndpoint._
import io.tofhir.common.model.Json4sSupport._
import io.tofhir.server.service.ConceptMapService
import io.tofhir.server.service.terminology.conceptmap.IConceptMapRepository

class ConceptMapEndpoint(conceptMapRepository: IConceptMapRepository) extends LazyLogging {

  val service: ConceptMapService = new ConceptMapService(conceptMapRepository)

  def route(request: ToFhirRestCall): Route = {
    pathPrefix(SEGMENT_CONCEPT_MAPS) {
      val terminologyId: String = request.terminologyId.get
      pathEndOrSingleSlash {
        getAllConceptMapsRoute(terminologyId)
      } ~ pathPrefix(Segment) { conceptMapId =>
        pathEndOrSingleSlash {
          getConceptMapRoute(terminologyId, conceptMapId)
        } ~ pathPrefix(SEGMENT_CONTENT) {
          pathEndOrSingleSlash {
            uploadDownloadConceptMapFileRoute(terminologyId, conceptMapId)
          }
        }
      }
    }
  }

  /**
   * Route to get all concept maps within a terminology
   *
   * @return
   */
  private def getAllConceptMapsRoute(terminologyId: String): Route = {
    get {
      complete {
        service.getConceptMaps(terminologyId) map { metadata =>
          StatusCodes.OK -> metadata
        }
      }
    }
  }

  /**
   * Route to get a concept map terminology
   *
   * @param terminologyId id of concept map terminology
   * @param conceptMapId  id of concept map
   * @return
   */
  private def getConceptMapRoute(terminologyId: String, conceptMapId: String): Route = {
    get {
      complete {
        service.getConceptMap(terminologyId, conceptMapId) map {
          case Some(conceptMap) => StatusCodes.OK -> conceptMap
          case None => StatusCodes.NotFound -> {
            throw ResourceNotFound("Concept map not found", s"Concept map  with id $conceptMapId not found")
          }
        }
      }
    }
  }

  /**
   * Route to upload/download a concept map file
   *
   * @param terminologyId id of concept map terminology
   * @param conceptMapId  id of concept map
   * @return
   */
  private def uploadDownloadConceptMapFileRoute(terminologyId: String, conceptMapId: String): Route = {
    post {
      fileUpload(ATTACHMENT) {
        case (fileInfo, byteSource) =>
          complete {
            service.uploadConceptMapFile(terminologyId, conceptMapId, byteSource) map {
              _ => StatusCodes.OK
            }
          }
      }
    } ~ get {
      parameterMap { queryParams =>
        complete {
          val pageNumber = queryParams.getOrElse("page", "1").toInt
          val pageSize = queryParams.getOrElse("size", "10").toInt
          service.downloadConceptMapFile(terminologyId, conceptMapId, pageNumber, pageSize) map {
            case (byteSource, totalRecords) =>
              HttpResponse(
                StatusCodes.OK,
                headers = List(RawHeader("X-Total-Count", totalRecords.toString)),
                entity = HttpEntity(ContentTypes.`text/csv(UTF-8)`, byteSource)
              )
          }
        }
      }
    }
  }

}

object ConceptMapEndpoint {
  val SEGMENT_CONCEPT_MAPS = "concept-maps"
}
