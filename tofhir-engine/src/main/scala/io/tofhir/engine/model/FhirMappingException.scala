package io.tofhir.engine.model

final case class FhirMappingException(private val reason: String, private val cause: Throwable = None.orNull)
  extends Exception(reason: String, cause: Throwable) {
}
