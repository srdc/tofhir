package io.tofhir.server.common.interceptor

import com.typesafe.scalalogging.LazyLogging
import akka.http.scaladsl.server.{Directives, ExceptionHandler}
import io.tofhir.server.common.model.{InternalError, ToFhirError, ToFhirRestCall}

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
      logger.error(s"toFHIR error encountered: ${e.toString}", e)
      e
    case e: Exception =>
      logger.error("Unexpected internal error", e)
      InternalError(s"Unexpected internal error: ${e.getClass.getName}", e.getMessage, Some(e))
  }
}
