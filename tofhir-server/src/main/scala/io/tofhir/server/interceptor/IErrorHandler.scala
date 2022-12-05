package io.tofhir.server.interceptor

import akka.http.scaladsl.server.{Directives, ExceptionHandler}
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.server.common.model.ToFhirRestCall
import io.tofhir.server.model.{InternalError, ToFhirError}

trait IErrorHandler extends LazyLogging {

  /**
   * Exception Handler object
   *
   * @return
   */
  def exceptionHandler(toFhirRestCall: ToFhirRestCall) =
    ExceptionHandler {
      case e: Exception => Directives.complete {
        val response = exceptionToResponse(e)
        toFhirRestCall.response = Some(response)
        response
      }
    }

  /**
   * Handling of exceptions by converting them to ToFhirResponse
   *
   * @return
   */
  private def exceptionToResponse: PartialFunction[Exception, ToFhirError] = {
    case e: ToFhirError =>
      logger.error(s"toFHIR error encountered.\n$e")
      e
    case e: Exception =>
      logger.error("Unexpected internal error", e)
      InternalError(s"Unexpected internal error: ${e.getClass.getName}", e.getMessage, Some(e))
  }
}
