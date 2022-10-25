package io.tofhir.server.common.model

import akka.http.scaladsl.model.{HttpMethod, Uri}

import java.time.Instant

/**
 * Object representing a REST request wrapper for toFHIR
 *
 * @param method    Http method
 * @param uri       Uri for the call
 * @param requestId Unique request identifier
 */
class ToFhirRestCall(val method: HttpMethod, val uri: Uri, val requestId: String) {

  /**
   * Time of the request
   */
  val requestTime: Instant = Instant.now()

  //TODO Resolved authentication context
  //var authzContext: Option[AuthzContext] = None

  /**
   * Project context for the call, if exists
   */
  var projectId: Option[String] = None

  /**
   * Request content
   */
  var request: Option[IToFhirRequest] = None

  /**
   * Response content
   */
  var response: Option[IToFhirResponse] = None
}
