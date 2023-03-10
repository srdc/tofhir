package io.tofhir.server.endpoint

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.config.ToFhirEngineConfig
import io.tofhir.server.endpoint.ConceptMapEndpoint.SEGMENT_CONCEPT_MAPS
import io.tofhir.server.endpoint.TerminologyServiceManagerEndpoint._
import io.tofhir.server.model.Json4sSupport._
import io.tofhir.server.model.TerminologySystem.TerminologyConceptMap
import io.tofhir.server.model.ToFhirRestCall
import io.tofhir.server.service.ConceptMapService

class ConceptMapEndpoint(toFhirEngineConfig: ToFhirEngineConfig) extends LazyLogging {

  val service: ConceptMapService = new ConceptMapService(toFhirEngineConfig.contextPath)

  def route(request: ToFhirRestCall): Route = {
    pathPrefix(SEGMENT_CONCEPT_MAPS) {
      val terminologyId: String = request.terminologyId.get
      pathEndOrSingleSlash {
        getAllConceptMapsRoute(terminologyId) ~ createConceptMapRoute(terminologyId)
      } ~ pathPrefix(Segment) { conceptMapId =>
        pathEndOrSingleSlash {
          getConceptMapRoute(terminologyId, conceptMapId) ~ putConceptMapRoute(terminologyId, conceptMapId) ~ deleteConceptMapRoute(terminologyId, conceptMapId)
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
   * Route to create a concept map within a terminology
   *
   * @return
   */
  private def createConceptMapRoute(terminologyId: String): Route = {
    post {
      entity(as[TerminologyConceptMap]) { conceptMap =>
        complete {
          service.createConceptMap(terminologyId, conceptMap) map { created =>
            StatusCodes.Created -> created
          }
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
          case None => StatusCodes.NotFound -> s"Concept map  with id $conceptMapId not found"
        }
      }
    }
  }

  /**
   * Route to put a concept map terminology
   *
   * @param terminologyId id of concept map terminology
   * @param conceptMapId  id of concept map
   * @return
   */
  private def putConceptMapRoute(terminologyId: String, conceptMapId: String): Route = {
    put {
      entity(as[TerminologyConceptMap]) { conceptMap =>
        complete {
          service.updateConceptMap(terminologyId, conceptMapId, conceptMap) map {
            terminologyConceptMap => StatusCodes.OK -> terminologyConceptMap
          }
        }
      }
    }
  }

  /**
   * Route to delete a concept map terminology
   *
   * @param terminologyId id of concept map terminology
   * @param conceptMapId  id of concept map
   * @return
   */
  private def deleteConceptMapRoute(terminologyId: String, conceptMapId: String): Route = {
    delete {
      complete {
        service.removeConceptMap(terminologyId, conceptMapId) map {
          _ => StatusCodes.NoContent
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
      complete {
        service.downloadConceptMapFile(terminologyId, conceptMapId) map { byteSource =>
          HttpResponse(StatusCodes.OK, entity = HttpEntity(ContentTypes.`text/csv(UTF-8)`, byteSource))
        }
      }
    }
  }

}

object ConceptMapEndpoint {
  val SEGMENT_CONCEPT_MAPS = "concept-maps"
}
