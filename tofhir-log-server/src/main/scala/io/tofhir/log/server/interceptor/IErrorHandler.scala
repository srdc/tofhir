package io.tofhir.log.server.interceptor

import akka.http.scaladsl.server.{Directives, ExceptionHandler}
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.server.common.model.{InternalError, ToFhirError}
import io.tofhir.server.common.model.ToFhirRestCall

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
        response.statusCode -> response.toString
      }
    }

  /**
   * Handling of exceptions by converting them to ToFhirResponse
   *
   * @return
   */
  private def exceptionToResponse: PartialFunction[Exception, ToFhirError] = {
    case e: ToFhirError =>
      logger.error(s"toFHIR error encountered.", e)
      e
    case e: Exception =>
      logger.error("Unexpected internal error", e)
      InternalError(s"Unexpected internal error: ${e.getClass.getName}", e.getMessage, Some(e))
  }
}
