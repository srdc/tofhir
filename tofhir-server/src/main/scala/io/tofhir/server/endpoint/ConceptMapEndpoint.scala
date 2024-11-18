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
import io.onfhir.definitions.common.model.Json4sSupport._
import io.tofhir.server.model.csv.CsvHeader
import io.tofhir.server.service.terminology.ConceptMapService
import io.tofhir.server.repository.terminology.conceptmap.IConceptMapRepository

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
        } ~ pathPrefix(SEGMENT_CONTENT) { // concept-maps/<concept-map-id>/content (paginated operations)
          pathEndOrSingleSlash {
            getOrSaveConceptMapContentRoute(terminologyId, conceptMapId)
          }
        } ~ pathPrefix(SEGMENT_HEADER) { // concept-maps/<concept-map-id>/header (CSV headers update)
          pathEndOrSingleSlash {
            updateConceptMapHeaderRoute(terminologyId, conceptMapId)
          }
        } ~ pathPrefix(SEGMENT_FILE) { // code-systems/<concept-map-id>/file (import/export as whole CSV file)
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
        service.getConceptMap(terminologyId, conceptMapId)
      }
    }
  }

  /**
   * Route to update concept map csv header
   *
   * Headers are passed as a list of `CsvHeader` objects and overwrite the existing headers in the first line of the CSV file.
   * The `CsvHeader` class represents each header with both its current name and previously saved name.
   * The rows are adjusted to match the new headers if necessary.
   *
   * e.g. 1:
   * Existing headers in a CSV:
   *  - CsvHeader(currentName = "header1", previousName = "header1")
   *  - CsvHeader(currentName = "header2", previousName = "header2")
   * New headers:
   * - CsvHeader(currentName = "header1", previousName = "header1")
   * - CsvHeader(currentName = "headerChanged", previousName = "header2")
   * - CsvHeader(currentName = "header3", previousName = "header3")
   * Rows under "header1" are preserved, rows belonging to "header2" are moved to "headerChanged", and "header3" is added with a default value for each row (`<header3>`).
   *
   * e.g. 2:
   * Existing headers in a CSV:
   * - CsvHeader(currentName = "header1", previousName = "header1")
   * - CsvHeader(currentName = "header2", previousName = "header2")
   * New headers:
   * - CsvHeader(currentName = "header2", previousName = "header2")
   * - CsvHeader(currentName = "header3", previousName = "header3")
   * Rows under "header1" are removed, rows under "header2" are preserved and shifted to the first column, and "header3" is added as the second column with a default value for each row (`<header3>`).
   *
   * @param terminologyId terminology id
   * @param conceptMapId concept map id
   * @return
   */
  private def updateConceptMapHeaderRoute(terminologyId: String, conceptMapId: String): Route = {
    post {
      entity(as[Seq[CsvHeader]]) { headers =>
        complete {
          service.updateConceptMapHeader(terminologyId, conceptMapId, headers) map { _ =>
            StatusCodes.OK
          }
        }
      }
    }
  }

  /**
   * Route to update or get a paginated concept map content
   * File to be updated is found by terminology id and concept map id
   * It updates/returns only the part of the CSV that corresponds to that page information. e.g:
   * If the CSV has 1000 records and the page size is 10, then the first page will have records from 1 to 10 excluding the header.
   * If the page number is 2, then the records from 11 to 20 will be returned/updated.
   *
   * @param terminologyId id of concept map terminology
   * @param conceptMapId  id of concept map
   * @return
   */
  private def getOrSaveConceptMapContentRoute(terminologyId: String, conceptMapId: String): Route = {
    /**
     * POST request to update part of the concept map content
     * byteSource contains the CSV content to be updated
     * Returns empty body with a total records header in case of success
     */
    post {
      fileUpload(ATTACHMENT) {
        case (fileInfo, byteSource) =>
          parameterMap { queryParams =>
            complete {
              val pageNumber = queryParams.getOrElse("page", "1").toInt
              val pageSize = queryParams.getOrElse("size", "10").toInt
              service.saveConceptMapContent(terminologyId, conceptMapId, byteSource, pageNumber, pageSize) map { totalRecords =>
                HttpResponse(
                  StatusCodes.OK,
                  headers = List(RawHeader("X-Total-Count", totalRecords.toString)),
                )
              }
            }
          }
      }

    /**
     * GET request to get a paginated concept map content
     * Returns the paginated CSV content with a total records header
     */
    } ~ get {
      parameterMap { queryParams =>
        complete {
          val pageNumber = queryParams.getOrElse("page", "1").toInt
          val pageSize = queryParams.getOrElse("size", "10").toInt
          service.getConceptMapContent(terminologyId, conceptMapId, pageNumber, pageSize) map {
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

  /**
   * Route to upload/download a concept map content
   * File to be uploaded/downloaded is found by terminology id and code system id
   * It uploads/downloads the whole CSV file as a single operation
   * Operations with large files may take a long time to complete
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
