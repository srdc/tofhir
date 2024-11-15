package io.tofhir.server.endpoint

import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.server.common.model.{ResourceNotFound, ToFhirRestCall}
import io.tofhir.server.endpoint.CodeSystemEndpoint.SEGMENT_CODE_SYSTEMS
import io.tofhir.server.endpoint.TerminologyServiceManagerEndpoint._
import io.onfhir.definitions.common.model.Json4sSupport._
import io.tofhir.server.model.csv.CsvHeader
import io.tofhir.server.service.terminology.CodeSystemService
import io.tofhir.server.repository.terminology.codesystem.ICodeSystemRepository

class CodeSystemEndpoint(codeSystemRepository: ICodeSystemRepository) extends LazyLogging {

  val service: CodeSystemService = new CodeSystemService(codeSystemRepository)

  def route(request: ToFhirRestCall): Route = {
    pathPrefix(SEGMENT_CODE_SYSTEMS) {
      val terminologyId: String = request.terminologyId.get
      pathEndOrSingleSlash {
        getAllCodeSystemsRoute(terminologyId)
      } ~ pathPrefix(Segment) { codeSystemId =>
        pathEndOrSingleSlash {
          getCodeSystemRoute(terminologyId, codeSystemId)
        } ~ pathPrefix(SEGMENT_CONTENT) { // code-systems/<code-system-id>/content (paginated operations)
          pathEndOrSingleSlash {
            getOrSaveCodeSystemContentRoute(terminologyId, codeSystemId)
          }
        } ~ pathPrefix(SEGMENT_HEADER) { // code-systems/<code-system-id>/header (CSV headers update)
          pathEndOrSingleSlash {
            updateCodeSystemHeaderRoute(terminologyId, codeSystemId)
          }
        } ~ pathPrefix(SEGMENT_FILE) { // code-systems/<code-system-id>/file (import/export as whole CSV file)
          pathEndOrSingleSlash {
            uploadDownloadCodeSystemFileRoute(terminologyId, codeSystemId)
          }
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
        service.getCodeSystems(terminologyId) map { metadata =>
          StatusCodes.OK -> metadata
        }
      }
    }
  }

  /**
   * Route to get a code system terminology
   *
   * @param terminologyId id of code system terminology
   * @param codeSystemId  id of code system
   * @return
   */
  private def getCodeSystemRoute(terminologyId: String, codeSystemId: String): Route = {
    get {
      complete {
        service.getCodeSystem(terminologyId, codeSystemId)
      }
    }
  }

  /**
   * Route to update code system CSV headers.
   *
   * Headers are passed as a list of `CsvHeader` objects, where each `CsvHeader` contains both the `currentName` (new header name) and `previousName` (the previously saved header name).
   * The existing headers in the first line of the CSV file are overwritten with the new headers provided. The rows are adjusted to match the new headers if necessary.
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
   * @param codeSystemId code system id
   * @return
   */
  private def updateCodeSystemHeaderRoute(terminologyId: String, codeSystemId: String): Route = {
    post {
      entity(as[Seq[CsvHeader]]) { headers =>
        complete {
          service.updateCodeSystemHeader(terminologyId, codeSystemId, headers) map { _ =>
            StatusCodes.OK
          }
        }
      }
    }
  }

  /**
   * Route to update or get a paginated code system content
   * File to be updated is found by terminology id and code system id
   * It updates/returns only the part of the CSV that corresponds to that page information. e.g:
   * If the CSV has 1000 records and the page size is 10, then the first page will have records from 1 to 10 excluding the header.
   * If the page number is 2, then the records from 11 to 20 will be returned/updated.
   *
   * @param terminologyId id of code system terminology
   * @param codeSystemId  id of code system
   * @return
   */
  private def getOrSaveCodeSystemContentRoute(terminologyId: String, codeSystemId: String): Route = {
    /**
     * POST request to update part of the code system content
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
              service.saveCodeSystemContent(terminologyId, codeSystemId, byteSource, pageNumber, pageSize) map { totalRecords =>
                HttpResponse(
                  StatusCodes.OK,
                  headers = List(RawHeader("X-Total-Count", totalRecords.toString)),
                )
              }
            }
          }
      }

    /**
     * GET request to get a paginated code system content
     * Returns the paginated CSV content with a total records header
     */
    } ~ get {
      parameterMap { queryParams =>
        complete {
          val pageNumber = queryParams.getOrElse("page", "1").toInt
          val pageSize = queryParams.getOrElse("size", "10").toInt
          service.getCodeSystemContent(terminologyId, codeSystemId, pageNumber, pageSize) map {
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
   * Route to upload/download code system content
   * File to be uploaded/downloaded is found by terminology id and code system id
   * It uploads/downloads the whole CSV file as a single operation
   * Operations with large files may take a long time to complete
   *
   * @param terminologyId id of code system terminology
   * @param codeSystemId  id of code system
   * @return
   */
  private def uploadDownloadCodeSystemFileRoute(terminologyId: String, codeSystemId: String): Route = {
    post {
      fileUpload(ATTACHMENT) {
        case (fileInfo, byteSource) =>
          complete {
            service.uploadCodeSystemFile(terminologyId, codeSystemId, byteSource) map {
              _ => StatusCodes.OK
            }
          }
      }
    } ~ get {
      complete {
        service.downloadCodeSystemFile(terminologyId, codeSystemId) map { byteSource =>
          HttpResponse(StatusCodes.OK, entity = HttpEntity(ContentTypes.`text/csv(UTF-8)`, byteSource))
        }
      }
    }
  }

}

object CodeSystemEndpoint {
  val SEGMENT_CODE_SYSTEMS = "code-systems"
}
