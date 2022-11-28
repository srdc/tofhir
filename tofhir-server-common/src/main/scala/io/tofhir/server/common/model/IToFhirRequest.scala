package io.tofhir.server.common.model

trait IToFhirRequest {
  /**
   * Unique identifier for the request (for tracing and correlation)
   */
  val requestId: String
}
