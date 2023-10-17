package io.tofhir.server.model

import io.tofhir.engine.model.FhirMappingError

import java.io.{PrintWriter, StringWriter}

/**
 * Any exception thrown by ToFHIR server
 */
abstract class ToFhirError extends Exception {
  /**
   * HTTP status code to return when this error occurs
   */
  val statusCode: Int
  /**
   * Type of the error
   */
  val `type`: String = s"https://tofhir.io/errors/${getClass.getSimpleName}"
  /**
   * Title of the error
   */
  val title: String
  /**
   * Details of the error
   */
  val detail: String

  /**
   * Inner exception
   */
  val cause: Option[Throwable] = None

  override def toString: String = {
    s"Status Code: $statusCode\n" +
      s"Type: ${`type`}\n" +
      s"Title: $title\n" +
      s"Detail: $detail\n" +
      s"Stack Trace: ${if (cause.isDefined) getStackTraceAsString(cause.get)}"
  }

  private def getStackTraceAsString(t: Throwable) = {
    val sw = new StringWriter
    t.printStackTrace(new PrintWriter(sw))
    sw.toString
  }

}

case class BadRequest(title: String, detail: String, override val cause: Option[Throwable] = None) extends ToFhirError {
  val statusCode = 400
}

case class AlreadyExists(title: String, detail: String) extends ToFhirError {
  val statusCode = 409
}

case class MappingExecutionError(title: String, detail: String) extends ToFhirError {
  val statusCode = 422

  def this(fhirMappingError: FhirMappingError) = {
    this(fhirMappingError.code, s"Description:${fhirMappingError.description} -- Expression:${fhirMappingError.expression.getOrElse("Not available.")}")
  }
}

case class ResourceNotFound(title: String, detail: String) extends ToFhirError {
  val statusCode = 404
}

case class InternalError(title: String, detail: String, override val cause: Option[Throwable] = None) extends ToFhirError {
  val statusCode = 500
}
