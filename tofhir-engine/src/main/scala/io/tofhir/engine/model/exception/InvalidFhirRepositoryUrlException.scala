package io.tofhir.engine.model.exception

/**
 * Exception thrown when attempting to use a URL that does not point to a valid FHIR repository.
 *
 * @param reason A description of the reason for the exception.
 * @param cause  The cause of this exception, if any.
 */
final case class InvalidFhirRepositoryUrlException(private val reason: String, private val cause: Throwable = None.orNull)
  extends Exception(reason: String, cause: Throwable) {
}
