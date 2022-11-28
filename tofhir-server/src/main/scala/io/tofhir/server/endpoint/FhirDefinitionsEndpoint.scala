package io.tofhir.server.endpoint

import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.server.common.endpoint.IToFhirEndpoint
import io.tofhir.server.common.model.ToFhirRestCall
import io.tofhir.server.endpoint.FhirDefinitionsEndpoint.DefinitionsQuery
import io.tofhir.server.fhir.FhirDefinitionsConfig
import io.tofhir.server.service.FhirDefinitionsService
import io.tofhir.server.model.Json4sSupport._

class FhirDefinitionsEndpoint(fhirDefinitionsConfig: FhirDefinitionsConfig) extends IToFhirEndpoint with LazyLogging {

  val service: FhirDefinitionsService = new FhirDefinitionsService(fhirDefinitionsConfig)

  def route(request: ToFhirRestCall): Route =
    pathPrefix("fhir-definitions") {
      pathEndOrSingleSlash {
        get {
          parameterMap { queryParams =>
            queryParams.get("q") match {
              case Some(v) =>
                DefinitionsQuery.withName(v) match {
                  case DefinitionsQuery.RESOURCE_TYPES => complete(service.getResourceTypes())
                  case DefinitionsQuery.PROFILES => complete("profiles queried")
                  case _ => complete(HttpResponse(StatusCodes.NotFound))
                }
              case None => complete("What to return?")
            }
          }
        }
      }
    }
}
object FhirDefinitionsEndpoint {
  object DefinitionsQuery extends Enumeration {
    type DefinitionsQuery = Value
    final val RESOURCE_TYPES = Value("rtypes")
    final val PROFILES = Value("profiles")
  }
}
