package io.tofhir.server.endpoint

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.config.ToFhirEngineConfig
import io.tofhir.server.endpoint.MappingContextEndpoint.{ATTACHMENT, SEGMENT_CONTENT, SEGMENT_CONTEXTS}
import io.tofhir.server.model.Json4sSupport._
import io.tofhir.server.model.ToFhirRestCall
import io.tofhir.server.service.MappingContextService
import io.tofhir.server.service.mappingcontext.IMappingContextRepository
import io.tofhir.server.service.project.IProjectRepository

class MappingContextEndpoint(mappingContextRepository: IMappingContextRepository) extends LazyLogging {

  val service: MappingContextService = new MappingContextService(mappingContextRepository)

  def route(request: ToFhirRestCall): Route = {
    pathPrefix(SEGMENT_CONTEXTS) {
      val projectId: String = request.projectId.get
      pathEndOrSingleSlash { // Operations on all mapping contexts
        getAllMappingContexts(projectId) ~ createMappingContext(request, projectId)
      } ~ // Operations on a single mapping contexts identified by its id
        pathPrefix(Segment) { id: String =>
          pathEndOrSingleSlash {
            deleteMappingContext(projectId, id)
          } ~ pathPrefix(SEGMENT_CONTENT) {
            pathEndOrSingleSlash {
              uploadDownloadMappingContextRoute(projectId, id)
            }
          }
        }
    }
  }

  private def getAllMappingContexts(projectId: String): Route = {
    get {
      complete {
        service.getAllMappingContext(projectId)
      }
    }
  }

  private def createMappingContext(request: ToFhirRestCall, projectId: String): Route = {
    post { // Create a new mapping context definition
      val mappingContextId = request.requestEntity.asInstanceOf[HttpEntity.Strict].data.utf8String
      complete {
        service.createMappingContext(projectId, mappingContextId) map { created =>
          StatusCodes.Created -> created
        }
      }
    }
  }

  private def deleteMappingContext(projectId: String, id: String): Route = {
    delete {
      complete {
        service.deleteMappingContext(projectId, id) map { _ =>
          StatusCodes.NoContent
        }
      }
    }
  }

  /**
   * Route to upload/download a mapping context file
   *
   * @param projectId project id
   * @param id        mapping context id
   * @return
   */
  private def uploadDownloadMappingContextRoute(projectId: String, id: String): Route = {
    post {
      fileUpload(ATTACHMENT) {
        case (fileInfo, byteSource) =>
          complete {
            service.uploadMappingContextFile(projectId, id, byteSource) map {
              _ => StatusCodes.OK
            }
          }
      }
    } ~ get {
      complete {
        service.downloadMappingContextFile(projectId, id) map { byteSource =>
          HttpResponse(StatusCodes.OK, entity = HttpEntity(ContentTypes.`text/csv(UTF-8)`, byteSource))
        }
      }
    }
  }

}

object MappingContextEndpoint {
  val SEGMENT_CONTEXTS = "mapping-contexts"
  val SEGMENT_CONTENT = "content"
  val ATTACHMENT = "attachment"
}



