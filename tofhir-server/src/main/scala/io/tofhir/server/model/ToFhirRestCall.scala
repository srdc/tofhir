package io.tofhir.server.model

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

  /**
   * Project context for the call, if exists
   */
  var projectId: Option[String] = None
}
