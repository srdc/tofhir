package io.tofhir.server.endpoint

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.config.ToFhirEngineConfig
import io.tofhir.server.endpoint.LocalTerminologyEndpoint.{SEGMENT_CONCEPTMAP, SEGMENT_TERMINOLOGY}
import io.tofhir.server.model.{LocalTerminology, TerminologyConceptMap, ToFhirRestCall}
import io.tofhir.server.service.{ConceptMapService, LocalTerminologyService}
import io.tofhir.server.model.Json4sSupport._

class LocalTerminologyEndpoint(toFhirEngineConfig: ToFhirEngineConfig) extends LazyLogging {

  val localTerminologyService: LocalTerminologyService = new LocalTerminologyService(toFhirEngineConfig.repositoryRootPath)

  val conceptMapService: ConceptMapService = new ConceptMapService(toFhirEngineConfig.repositoryRootPath)

  def route(request: ToFhirRestCall): Route = {
    pathPrefix(SEGMENT_TERMINOLOGY) {
      pathEndOrSingleSlash { // Operations on all schemas
        get { // Search/(Retrieve all) local terminology metadata (id, name, description, folderPath)
          complete {
            localTerminologyService.getAllMetadata map { metadata =>
              StatusCodes.OK -> metadata
            }
          }
        } ~ post { // Create a new local terminology service
          entity(as[LocalTerminology]) { terminology =>
            complete {
              localTerminologyService.createTerminologyService(terminology) map { created =>
                StatusCodes.Created -> created
              }
            }
          }
        }
      } ~ pathPrefix(Segment) { terminologyId: String =>
        pathEndOrSingleSlash {
          get { // Retrieve a local terminology service by id
            complete {
              localTerminologyService.getTerminologyServiceById(terminologyId) map {
                terminology => StatusCodes.OK -> terminology
              }
            }
          } ~ put { // Update a local terminology service by id
            entity(as[LocalTerminology]) { terminology =>
              complete {
                localTerminologyService.updateTerminologyServiceById(terminologyId, terminology) map {
                  terminology => StatusCodes.OK -> terminology
                }
              }
            }
          } ~ delete { // Remove a local terminology service by id
            complete {
              localTerminologyService.removeTerminologyServiceById(terminologyId) map {
                _ => StatusCodes.OK
              }
            }
          }
        } ~ pathPrefix (SEGMENT_CONCEPTMAP) {
          pathEndOrSingleSlash { // Operations on concept maps
            get { // List local concept maps metadata within a terminology
              complete {
                conceptMapService.getConceptMaps(terminologyId) map { metadata =>
                  StatusCodes.OK -> metadata
                }
              }
            } ~ post { // Create a new concept map within a terminology
              entity(as[TerminologyConceptMap]) { conceptMap =>
                complete {
                  conceptMapService.createConceptMap(terminologyId, conceptMap) map { created =>
                    StatusCodes.Created -> created
                  }
                }
              }
            }
          } ~ pathPrefix(Segment) { conceptMapId: String =>
            pathEndOrSingleSlash {
              get { // Retrieve a concept map within a terminology
                complete {
                  conceptMapService.getConceptMap(terminologyId, conceptMapId) map {
                    terminology => StatusCodes.OK -> terminology
                  }
                }
              } ~ put { // Update a concept map within a terminology
                entity(as[TerminologyConceptMap]) { conceptMap =>
                  complete {
                    conceptMapService.updateConceptMap(terminologyId, conceptMapId, conceptMap) map {
                      terminology => StatusCodes.OK -> conceptMap
                    }
                  }
                }
              } ~ delete { // Delete a concept map within a terminology
                complete {
                  conceptMapService.removeConceptMap(terminologyId, conceptMapId) map {
                    _ => StatusCodes.OK
                  }
                }
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
  val SEGMENT_CONCEPTMAP = "conceptmap"
}

