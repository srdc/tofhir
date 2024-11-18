package io.tofhir.server.endpoint

import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.server.common.model.ToFhirRestCall
import io.tofhir.server.endpoint.MappingContextEndpoint.{ATTACHMENT, SEGMENT_CONTENT, SEGMENT_CONTEXTS, SEGMENT_FILE, SEGMENT_HEADER}
import io.onfhir.definitions.common.model.Json4sSupport._
import io.tofhir.server.model.csv.CsvHeader
import io.tofhir.server.repository.mappingContext.IMappingContextRepository
import io.tofhir.server.service.MappingContextService

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
            deleteMappingContext(projectId, id) // Delete a mapping context: mapping-contexts/<mapping-context-id>
          } ~ pathPrefix(SEGMENT_CONTENT) {
            pathEndOrSingleSlash {
              getOrSaveMappingContextContentRoute(projectId, id) // mapping-contexts/<mapping-context-id>/content (paginated operations)
            }
          } ~ pathPrefix(SEGMENT_HEADER) { // mapping-contexts/<mapping-context-id>/header (CSV headers update)
            pathEndOrSingleSlash {
              updateMappingContextHeaderRoute(projectId, id)
            }
          } ~ pathPrefix(SEGMENT_FILE) { // mapping-contexts/<mapping-context-id>/file (import/export as whole CSV file)
            pathEndOrSingleSlash {
              uploadDownloadMappingContextRoute(projectId, id)
            }
          }
        }
    }
  }

  /**
   * Route to get all mapping contexts
   * @param projectId
   * @return
   */
  private def getAllMappingContexts(projectId: String): Route = {
    get {
      complete {
        service.getAllMappingContext(projectId)
      }
    }
  }

  /**
   * Route to create a new mapping context
   * @param request
   * @param projectId
   * @return
   */
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

  /**
   * Route to delete a mapping context
   * @param projectId
   * @param id
   * @return
   */
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
   * Route to update the mapping context CSV headers
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
   * @param projectId project id
   * @param id mapping context id
   * @return
   */
  private def updateMappingContextHeaderRoute(projectId: String, id: String): Route = {
    post {
      entity(as[Seq[CsvHeader]]) { headers =>
        complete {
          service.updateMappingContextHeader(projectId, id, headers) map { _ =>
            StatusCodes.OK
          }
        }
      }
    }
  }

  /**
   * Route to get or post a paginated mapping context content
   *
   * File to be updated is found by project id and context id
   * It updates/returns only the part of the CSV that corresponds to that page information. e.g:
   * If the CSV has 1000 records and the page size is 10, then the first page will have records from 1 to 10 excluding the header.
   * If the page number is 2, then the records from 11 to 20 will be returned/updated.
   *
   * @param projectId project id
   * @param id        mapping context id
   * @return
   */
  private def getOrSaveMappingContextContentRoute(projectId: String, id: String): Route = {
    /**
     * POST request to update part of the mapping context content
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
              service.saveMappingContextContent(projectId, id, byteSource, pageNumber, pageSize) map { totalRecords =>
                HttpResponse(
                  StatusCodes.OK,
                  headers = List(RawHeader("X-Total-Count", totalRecords.toString)),
                )
              }
            }
          }
      }
      /**
       * GET request to get a paginated mapping context content
       * Returns the paginated CSV content with a total records header
       */
    } ~ get {
      parameterMap { queryParams =>
        complete {
          val pageNumber = queryParams.getOrElse("page", "1").toInt
          val pageSize = queryParams.getOrElse("size", "10").toInt
          service.getMappingContextContent(projectId, id, pageNumber, pageSize) map {
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
   * Route to upload/download a mapping context file
   * File to be uploaded/downloaded is found by project id and mapping context id
   * It uploads/downloads the whole CSV file as a single operation
   * Operations with large files may take a long time to complete
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
  val SEGMENT_HEADER = "header"
  val SEGMENT_FILE = "file"
  val ATTACHMENT = "attachment"
}



