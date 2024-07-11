package io.tofhir.engine.model.exception

/**
 * Exception to throw in case a problem during the initialization of the engine
 * @param reason
 * @param cause
 */
final case class EngineInitializationException(private val reason: String, private val cause: Throwable = None.orNull)
  extends Exception(reason: String, cause: Throwable) {
}
