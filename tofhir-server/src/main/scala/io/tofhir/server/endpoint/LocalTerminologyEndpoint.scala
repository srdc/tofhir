package io.tofhir.server.endpoint

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.config.ToFhirEngineConfig
import io.tofhir.server.endpoint.LocalTerminologyEndpoint.SEGMENT_TERMINOLOGY
import io.tofhir.server.model.{LocalTerminology, ToFhirRestCall}
import io.tofhir.server.service.LocalTerminologyService
import io.tofhir.server.model.Json4sSupport._

class LocalTerminologyEndpoint(toFhirEngineConfig: ToFhirEngineConfig) extends LazyLogging {

  val service: LocalTerminologyService = new LocalTerminologyService(toFhirEngineConfig.repositoryRootPath)

  def route(request: ToFhirRestCall): Route = {
    pathPrefix(SEGMENT_TERMINOLOGY) {
      pathEndOrSingleSlash { // Operations on all schemas
        get { // Search/(Retrieve all) local terminology metadata (id, name, description, folderPath)
          complete {
            service.getAllMetadata map { metadata =>
              StatusCodes.OK -> metadata
            }
          }
        } ~ post { // Create a new local terminology service
          entity(as[LocalTerminology]) { terminology =>
            complete {
              service.createTerminologyService(terminology) map { created =>
                StatusCodes.Created -> created
              }
            }
          }
        }
      } ~ pathPrefix(Segment) { terminologyId: String =>
        pathEndOrSingleSlash {
          get { // Retrieve a local terminology service by id
            complete {
              service.getTerminologyServiceById(terminologyId) map {
                terminology => StatusCodes.OK -> terminology
              }
            }
          } ~ put {
            entity(as[LocalTerminology]) { terminology =>
              complete {
                service.updateTerminologyServiceById(terminologyId, terminology) map {
                  terminology => StatusCodes.OK -> terminology
                }
              }
            }
          } ~ delete {
            complete {
              service.removeTerminologyServiceById(terminologyId) map {
                _ => StatusCodes.OK
              }
            }
          }
        }
      }
    }
  }

}

object LocalTerminologyEndpoint {
  val SEGMENT_TERMINOLOGY = "terminology"
}

