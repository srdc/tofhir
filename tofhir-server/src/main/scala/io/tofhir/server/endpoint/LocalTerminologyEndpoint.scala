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
      // operations on all terminologies
      pathEndOrSingleSlash {
        createLocalTerminologyRoute() ~ getAllLocalTerminologiesRoute
      } ~ // operations on individual terminologies
        pathPrefix(Segment) { terminologyId: String =>
          pathEndOrSingleSlash {
            getLocalTerminologyRoute(terminologyId) ~ putLocalTerminologyRoute(terminologyId) ~ deleteLocalTerminologyRoute(terminologyId)
          } ~ pathPrefix(SEGMENT_CONCEPTMAP) {
            pathEndOrSingleSlash {
              getAllConceptMapsRoute(terminologyId) ~ createConceptMapRoute(terminologyId)
            } ~ pathPrefix(Segment) { conceptMapId =>
              pathEndOrSingleSlash {
                getConceptMapRoute(terminologyId, conceptMapId) ~ putConceptMapRoute(terminologyId, conceptMapId) ~ deleteConceptMapRoute(terminologyId, conceptMapId)
              } ~ pathPrefix(SEGMENT_FILE) {
                pathEndOrSingleSlash {
                  uploadDownloadConceptMapFileRoute(terminologyId, conceptMapId)
                }
              }
            }
          } ~ pathPrefix(SEGMENT_CODESYSTEM) {
            pathEndOrSingleSlash {
              getAllCodeSystemsRoute(terminologyId) ~ createCodeSystemRoute(terminologyId)
            } ~ pathPrefix(Segment) { codeSystemId =>
              pathEndOrSingleSlash {
                getCodeSystemRoute(terminologyId, codeSystemId) ~ putCodeSystemRoute(terminologyId, codeSystemId) ~ deleteCodeSystemRoute(terminologyId, codeSystemId)
              } ~ pathPrefix(SEGMENT_FILE) {
                pathEndOrSingleSlash {
                  uploadDownloadCodeSystemFileRoute(terminologyId, codeSystemId)
                }
              }
            }
          }
        }
    }
  }

  /**
   * Route to get all local terminology
   *
   * @return
   */
  private def getAllLocalTerminologiesRoute: Route = {
    get {
      complete {
        localTerminologyService.getAllMetadata
      }
    }
  }

  /**
   * Route to create a local terminology server
   *
   * @return
   */
  private def createLocalTerminologyRoute(): Route = {
    post {
      entity(as[LocalTerminology]) { localTerminology =>
        complete {
          localTerminologyService.createTerminologyService(localTerminology) map { createdTerminology =>
            StatusCodes.Created -> createdTerminology
          }
        }
      }
    }
  }

  /**
   * Route to get a local terminology
   *
   * @param terminologyId id of local terminology
   * @return
   */
  private def getLocalTerminologyRoute(terminologyId: String): Route = {
    get {
      complete {
        localTerminologyService.getTerminologyServiceById(terminologyId) map {
          terminology => StatusCodes.OK -> terminology
        }
      }
    }
  }

  /**
   * Route to put a local terminology
   *
   * @param terminologyId id of local terminology
   * @return
   */
  private def putLocalTerminologyRoute(terminologyId: String): Route = {
    put {
      entity(as[LocalTerminology]) { terminology =>
        complete {
          localTerminologyService.updateTerminologyServiceById(terminologyId, terminology) map {
            terminology => StatusCodes.OK -> terminology
          }
        }
      }
    }
  }

  /**
   * Route to delete a local terminology
   *
   * @param terminologyId id of local terminology
   * @return
   */
  private def deleteLocalTerminologyRoute(terminologyId: String): Route = {
    delete {
      complete {
        localTerminologyService.removeTerminologyServiceById(terminologyId) map {
          _ => StatusCodes.OK
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
        conceptMapService.getConceptMaps(terminologyId) map { metadata =>
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
          conceptMapService.createConceptMap(terminologyId, conceptMap) map { created =>
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
   * @param conceptMapId id of concept map
   * @return
   */
  private def getConceptMapRoute(terminologyId: String, conceptMapId: String): Route = {
    get {
      complete {
        conceptMapService.getConceptMap(terminologyId, conceptMapId) map {
          terminology => StatusCodes.OK -> terminology
        }
      }
    }
  }

  /**
   * Route to put a concept map terminology
   *
   * @param terminologyId id of concept map terminology
   * @param conceptMapId id of concept map
   * @return
   */
  private def putConceptMapRoute(terminologyId: String, conceptMapId: String): Route = {
    put {
      entity(as[TerminologyConceptMap]) { conceptMap =>
        complete {
          conceptMapService.updateConceptMap(terminologyId, conceptMapId, conceptMap) map {
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
   * @param conceptMapId id of concept map
   * @return
   */
  private def deleteConceptMapRoute(terminologyId: String, conceptMapId: String): Route = {
    delete {
      complete {
        conceptMapService.removeConceptMap(terminologyId, conceptMapId) map {
          _ => StatusCodes.OK
        }
      }
    }
  }

  /**
   * Route to upload/download a concept map file
   *
   * @param terminologyId id of concept map terminology
   * @param conceptMapId id of concept map
   * @return
   */
  private def uploadDownloadConceptMapFileRoute(terminologyId: String, conceptMapId: String): Route = {
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

  /**
   * Route to get all code systems within a terminology
   *
   * @return
   */
  private def getAllCodeSystemsRoute(terminologyId: String): Route = {
    get {
      complete {
        codeSystemService.getCodeSystems(terminologyId) map { metadata =>
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
          codeSystemService.createCodeSystem(terminologyId, codeSystem) map { created =>
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
   * @param codeSystemId id of code system
   * @return
   */
  private def getCodeSystemRoute(terminologyId: String, codeSystemId: String): Route = {
    get {
      complete {
        codeSystemService.getCodeSystem(terminologyId, codeSystemId) map {
          terminology => StatusCodes.OK -> terminology
        }
      }
    }
  }

  /**
   * Route to put a code system terminology
   *
   * @param terminologyId id of code system terminology
   * @param codeSystemId id of code system
   * @return
   */
  private def putCodeSystemRoute(terminologyId: String, codeSystemId: String): Route = {
    put {
      entity(as[TerminologyCodeSystem]) { codeSystem =>
        complete {
          codeSystemService.updateCodeSystem(terminologyId, codeSystemId, codeSystem) map {
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
   * @param codeSystemId id of code system
   * @return
   */
  private def deleteCodeSystemRoute(terminologyId: String, codeSystemId: String): Route = {
    delete {
      complete {
        codeSystemService.removeCodeSystem(terminologyId, codeSystemId) map {
          _ => StatusCodes.OK
        }
      }
    }
  }

  /**
   * Route to upload/download a code system file
   * @param terminologyId id of code system terminology
   * @param codeSystemId id of code system
   *@return
   */
  private def uploadDownloadCodeSystemFileRoute(terminologyId: String, codeSystemId: String): Route = {
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

object LocalTerminologyEndpoint {
  val SEGMENT_TERMINOLOGY = "terminology"
  val SEGMENT_CONCEPTMAP = "conceptmap"
  val SEGMENT_CODESYSTEM = "codesystem"
  val SEGMENT_FILE = "file"
  val ATTACHMENT = "attachment"
}

