package io.tofhir.engine.model.exception

/**
 * Exception indicating the mapping job execution stopped externally by the user.
 *
 * @param reason
 * @param cause
 */
final case class FhirMappingJobStoppedException(private val reason: String, private val cause: Throwable = None.orNull)
  extends Exception(reason: String, cause: Throwable) {
}
