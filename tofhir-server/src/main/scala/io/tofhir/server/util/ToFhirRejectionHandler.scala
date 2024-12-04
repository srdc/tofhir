package io.tofhir.server.util

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import io.tofhir.server.common.interceptor.ICORSHandler
import io.tofhir.server.common.model.{BadRequest, MethodForbidden, RequestTimeout, ResourceNotFound, UnsupportedMediaType}

object ToFhirRejectionHandler extends ICORSHandler {

  /**
   * Custom rejection handler to send proper error message to front-end on rejections.
   */
  private val rejectionHandler: RejectionHandler =
    RejectionHandler.newBuilder()

      /**
       * Handles the cases where the type of the request data is wrong. Ex: String instead of a terminology system model.
       */
      .handle {
        case UnsupportedRequestContentTypeRejection(supportedTypes) =>
          complete(StatusCodes.UnsupportedMediaType -> UnsupportedMediaType("Unsupported payload format", s"Server refuses to accept the request because the type of the request data is not in a supported format").toString)
      }

      /**
       * Handles the malformed content exception. Some examples are:
       * - No id field in an imported file
       * - No usable value for id cause is a brief description of the exception
       * - Any malformed content for service
       */
      .handle {
        case MalformedRequestContentRejection(message, cause) =>
          complete(StatusCodes.BadRequest -> BadRequest("Malformed request content", message).toString)
        case ValidationRejection(message, _) =>
          complete(StatusCodes.BadRequest -> BadRequest("Request is not valid", message).toString)
      }

      /**
       * Handles all types of method rejections, i.e the REST method is not applicable on the given URL.
       */
      .handleAll[MethodRejection] { methodRejections =>
        // supportedMethods are the applicable methods on the given URL
        val supportedMethods = methodRejections.map(_.supported.name)
        complete(StatusCodes.MethodNotAllowed -> MethodForbidden("Method not allowed", s"Server refuses to accept the request because request method is not allowed in this URL. Supported methods: ${supportedMethods.mkString(", ")}.").toString)
      }

      /**
       * Handles when the given URL is not mapped to anywhere in API
       * This case has a special representation as handleNotFound
       */
      .handleNotFound {
        extractUri { requestUrl  =>
          complete(StatusCodes.NotFound -> ResourceNotFound("Url not found", s"${requestUrl} does not exist in this API.").toString)
        }
      }

      /**
       * Handle the rest of the possible rejection types. Send the name of the rejections.
       */
      .handleAll[Rejection] { rejections =>
        val rejectionMessages = rejections.map(_.toString).mkString(", ")
        complete(StatusCodes.BadRequest -> BadRequest("Request rejected.", s"Rejection reason: $rejectionMessages"))
      }
      .result()

  def getRejectionHandler(): RejectionHandler = {
    rejectionHandler
  }

  /**
   * Handles the timeout exception. This is a custom response to the timeout exception.
   */
  def timeoutResponseHandler(): HttpResponse = {
    val error = RequestTimeout(
      title = "Request Timeout",
      detail = "The server could not complete the request in time. Try increasing the timeout config and try again."
    )
    addCORSHeaders(
      HttpResponse(
        status = StatusCodes.RequestTimeout,
        entity = HttpEntity(ContentTypes.`application/json`, error.toString)
      )
    )
  }
}
