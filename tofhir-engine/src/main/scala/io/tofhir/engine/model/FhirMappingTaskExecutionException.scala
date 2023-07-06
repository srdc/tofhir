package io.tofhir.engine.model

/**
 * Exception to be thrown when there is an error during mapping task execution.
 *
 * @param msg the message
 * */
final case class FhirMappingTaskExecutionException(private val msg: String)
  extends Exception(msg: String, null) {
}