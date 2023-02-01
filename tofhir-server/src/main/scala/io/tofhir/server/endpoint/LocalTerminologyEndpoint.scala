package io.tofhir.server.endpoint

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives.{complete, _}
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.config.ToFhirEngineConfig
import io.tofhir.server.endpoint.LocalTerminologyEndpoint._
import io.tofhir.server.model.Json4sSupport._
import io.tofhir.server.model.{LocalTerminology, TerminologyCodeSystem, TerminologyConceptMap, ToFhirRestCall}
import io.tofhir.server.service.{CodeSystemService, ConceptMapService, LocalTerminologyService}

class LocalTerminologyEndpoint(toFhirEngineConfig: ToFhirEngineConfig) extends LazyLogging {

  val localTerminologyService: LocalTerminologyService = new LocalTerminologyService(toFhirEngineConfig.repositoryRootPath)

  val conceptMapService: ConceptMapService = new ConceptMapService(toFhirEngineConfig.repositoryRootPath)

  val codeSystemService: CodeSystemService = new CodeSystemService(toFhirEngineConfig.repositoryRootPath)

  def route(request: ToFhirRestCall): Route = {
    pathPrefix(SEGMENT_TERMINOLOGY) {
      pathEndOrSingleSlash { // Operations on all terminology
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
        } ~ pathPrefix(SEGMENT_CONCEPTMAP) { // Operations on concept maps
          pathEndOrSingleSlash {
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
                      terminologyConceptMap => StatusCodes.OK -> terminologyConceptMap
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
            } ~ pathPrefix(SEGMENT_FILE) {
              pathEndOrSingleSlash {
                post {
                  fileUpload(ATTACHMENT) {
                    case (fileInfo, byteSource) =>
                      complete {
                        conceptMapService.uploadConceptMapFile(terminologyId, conceptMapId, byteSource) map {
                          _ => StatusCodes.OK
                        }
                      }
                  }
                } ~ get {
                  complete {
                    conceptMapService.downloadConceptMapFile(terminologyId, conceptMapId) map { byteSource =>
                      HttpResponse(StatusCodes.OK, entity = HttpEntity(ContentTypes.`text/csv(UTF-8)`, byteSource))
                    }
                  }
                }
              }
            }
          }
        } ~ pathPrefix(SEGMENT_CODESYSTEM) { // Operations on code system
          pathEndOrSingleSlash {
            get { // List local code systems metadata within a terminology
              complete {
                codeSystemService.getCodeSystems(terminologyId) map { metadata =>
                  StatusCodes.OK -> metadata
                }
              }
            } ~ post { // Create a new code system within a terminology
              entity(as[TerminologyCodeSystem]) { codeSystem =>
                complete {
                  codeSystemService.createCodeSystem(terminologyId, codeSystem) map { created =>
                    StatusCodes.Created -> created
                  }
                }
              }
            }
          } ~ pathPrefix(Segment) { codeSystemId: String =>
            pathEndOrSingleSlash {
              get { // Retrieve a code system within a terminology
                complete {
                  codeSystemService.getCodeSystem(terminologyId, codeSystemId) map {
                    terminology => StatusCodes.OK -> terminology
                  }
                }
              } ~ put { // Update a code system within a terminology
                entity(as[TerminologyCodeSystem]) { codeSystem =>
                  complete {
                    codeSystemService.updateCodeSystem(terminologyId, codeSystemId, codeSystem) map {
                      terminologyCodeSystem => StatusCodes.OK -> terminologyCodeSystem
                    }
                  }
                }
              } ~ delete { // Delete a code system within a terminology
                complete {
                  codeSystemService.removeCodeSystem(terminologyId, codeSystemId) map {
                    _ => StatusCodes.OK
                  }
                }
              }
            } ~ pathPrefix(SEGMENT_FILE) {
              pathEndOrSingleSlash {
                post {
                  fileUpload(ATTACHMENT) {
                    case (fileInfo, byteSource) =>
                      complete {
                        codeSystemService.uploadCodeSystemFile(terminologyId, codeSystemId, byteSource) map {
                          _ => StatusCodes.OK
                        }
                      }
                  }
                } ~ get {
                  complete {
                    codeSystemService.downloadCodeSystemFile(terminologyId, codeSystemId) map { byteSource =>
                      HttpResponse(StatusCodes.OK, entity = HttpEntity(ContentTypes.`text/csv(UTF-8)`, byteSource))
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

}

object LocalTerminologyEndpoint {
  val SEGMENT_TERMINOLOGY = "terminology"
  val SEGMENT_CONCEPTMAP = "conceptmap"
  val SEGMENT_CODESYSTEM = "codesystem"
  val SEGMENT_FILE = "file"
  val ATTACHMENT = "attachment"
}

