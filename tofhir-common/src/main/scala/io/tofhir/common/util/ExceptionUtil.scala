package io.tofhir.common.util

object ExceptionUtil {
  /**
   * Accumulates a single message from messages of exceptions that are reached through the cause relationship
   *
   * @param exception top-level exception
   * @return
   */
  def extractExceptionMessages(exception: Throwable): String = {
    var allExceptions: List[Throwable] = List(exception)

    var iterator: Throwable = exception
    while (iterator.getCause != null) {
      allExceptions :+= iterator.getCause
      iterator = iterator.getCause
    }
    allExceptions
      .map(_.getMessage)
      .filter(message => message != null && message.nonEmpty)
      .mkString("\n")
  }
}
