package io.tofhir.server.endpoint

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.config.ToFhirEngineConfig
import io.tofhir.engine.model.FhirMapping
import io.tofhir.server.endpoint.MappingEndpoint.SEGMENT_MAPPING
import io.tofhir.server.model.Json4sSupport._
import io.tofhir.server.model.ToFhirRestCall
import io.tofhir.server.service.MappingService
import io.tofhir.server.service.project.IProjectRepository

class MappingEndpoint(toFhirEngineConfig: ToFhirEngineConfig, projectRepository: IProjectRepository) extends LazyLogging {

  val service: MappingService = new MappingService(toFhirEngineConfig.mappingRepositoryFolderPath, projectRepository)

  def route(request: ToFhirRestCall): Route = {
    pathPrefix(SEGMENT_MAPPING) {
      val projectId: String = request.projectId.get
      pathEndOrSingleSlash { // Operations on all mappings
        getAllMappings(projectId) ~ createMapping(projectId)
      } ~ // Operations on a single mapping identified by its id
        pathPrefix(Segment) { id: String =>
          getMapping(projectId, id) ~ updateMapping(projectId, id) ~ deleteMapping(projectId, id)
        }
    }
  }

  private def getAllMappings(projectId: String): Route = {
    get {
      complete {
        service.getAllMetadata(projectId)
      }
    }
  }

  private def createMapping(projectId: String): Route = {
    post { // Create a new mapping definition
      entity(as[FhirMapping]) { fhirMapping =>
        complete {
          service.createMapping(projectId, fhirMapping) map { created =>
            StatusCodes.Created -> created
          }
        }
      }
    }
  }

  private def getMapping(projectId: String, id: String): Route = {
    get {
      complete {
        service.getMapping(projectId, id) map {
          case Some(fhirMapping) => StatusCodes.OK -> fhirMapping
          case None => StatusCodes.NotFound -> s"Mapping with name $id not found"
        }
      }
    }
  }

  private def updateMapping(projectId: String, id: String): Route = {
    put {
      entity(as[FhirMapping]) { fhirMapping =>
        complete {
          service.putMapping(projectId, id, fhirMapping) map { _ =>
            StatusCodes.OK -> fhirMapping
          }
        }
      }
    }
  }

  private def deleteMapping(projectId: String, id: String): Route = {
    delete {
      complete {
        service.deleteMapping(projectId, id) map { _ =>
          StatusCodes.NoContent
        }
      }
    }
  }

}

object MappingEndpoint {
  val SEGMENT_MAPPING = "mapping"
}



