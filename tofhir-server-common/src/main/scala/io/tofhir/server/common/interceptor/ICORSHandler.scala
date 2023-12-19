package io.tofhir.server.common.interceptor

import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.model.{HttpMethods, HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.directives.BasicDirectives
import akka.http.scaladsl.server.{Directive0, Directives, Route}

/**
 * Cors Handler for webserver modules
 */
trait ICORSHandler extends BasicDirectives {
  /**
   * Fixed CORS Response headers
   */
  private val corsResponseHeaders = List(
    `Access-Control-Allow-Origin`.*,
    `Access-Control-Allow-Credentials`(true),
    `Access-Control-Allow-Headers`("Origin, X-Requested-With, X-Correlation-Id, Content-Type, Accept, Accept-Encoding, Accept-Language, Authorization, Host, Referer, User-Agent, Link"),
    `Access-Control-Max-Age`(1728000),
    `Access-Control-Expose-Headers`("Location", "Link", ICORSHandler.X_TOTAL_COUNT_HEADER)
  )

  //this directive adds access control headers to normal responses
  private def addAccessControlHeaders(): Directive0 = {
    Directives.respondWithHeaders(corsResponseHeaders)
  }

  //this handles preflight OPTIONS requests.
  private def preflightRequestHandler: Route = options {
    complete(
      HttpResponse(StatusCodes.OK)
        .withHeaders(`Access-Control-Allow-Methods`(HttpMethods.OPTIONS, HttpMethods.HEAD, HttpMethods.POST, HttpMethods.PUT, HttpMethods.GET, HttpMethods.DELETE, HttpMethods.PATCH))
    )
  }

  // Wrap the Route with this method to enable adding of CORS headers
  def corsHandler(r: Route): Route = addAccessControlHeaders() {
    preflightRequestHandler ~ r
  }

  // Helper method to add CORS headers to HttpResponse
  // preventing duplication of CORS headers across code
  def addCORSHeaders(response: HttpResponse): HttpResponse =
    response.withHeaders(corsResponseHeaders)

}

/**
 * Keeps the custom header names
 */
object ICORSHandler {
  val X_TOTAL_COUNT_HEADER = "X-Total-Count" // Header to return the total number of resources
}