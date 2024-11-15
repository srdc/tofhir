package io.tofhir.server.endpoint

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.model.FhirMapping
import io.tofhir.server.common.model.{ResourceNotFound, ToFhirRestCall}
import io.tofhir.server.endpoint.MappingEndpoint.SEGMENT_MAPPINGS
import io.onfhir.definitions.common.model.Json4sSupport._
import io.tofhir.server.repository.job.IJobRepository
import io.tofhir.server.repository.mapping.IMappingRepository
import io.tofhir.server.service.MappingService

class MappingEndpoint(mappingRepository: IMappingRepository, jobRepository: IJobRepository) extends LazyLogging {

  val service: MappingService = new MappingService(mappingRepository, jobRepository)

  def route(request: ToFhirRestCall): Route = {
    pathPrefix(SEGMENT_MAPPINGS) {
      val projectId: String = request.projectId.get
      pathEndOrSingleSlash { // Operations on all mappings
        getAllMappings(projectId) ~ createMapping(projectId)
      } ~ // Operations on a single mapping identified by its id
        pathPrefix(Segment) { id: String =>
          getMapping(projectId, id) ~ putMapping(projectId, id) ~ deleteMapping(projectId, id)
        }
    }
  }

  private def getAllMappings(projectId: String): Route = {
    get {
      complete {
        service.getAllMappings(projectId)
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

  private def getMapping(projectId: String, mappingId: String): Route = {
    get {
      complete {
        service.getMapping(projectId, mappingId) map {
          case Some(fhirMapping) => StatusCodes.OK -> fhirMapping
          case None => StatusCodes.NotFound -> {
            throw ResourceNotFound("Mapping not found", s"Mapping with name $mappingId not found")
          }
        }
      }
    }
  }

  private def putMapping(projectId: String, mappingId: String): Route = {
    put {
      entity(as[FhirMapping]) { fhirMapping =>
        complete {
          service.updateMapping(projectId, mappingId, fhirMapping) map { res: FhirMapping =>
            StatusCodes.OK -> res
          }
        }
      }
    }
  }

  private def deleteMapping(projectId: String, mappingId: String): Route = {
    delete {
      complete {
        service.deleteMapping(projectId, mappingId) map { _ =>
          StatusCodes.NoContent
        }
      }
    }
  }

}

object MappingEndpoint {
  val SEGMENT_MAPPINGS = "mappings"
}



