package io.tofhir.server.endpoint

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.server.common.endpoint.IToFhirEndpoint
import io.tofhir.server.common.model.ToFhirRestCall


class FhirDefinitionsEndpoint extends IToFhirEndpoint with LazyLogging {
  def route(request: ToFhirRestCall): Route =
    pathPrefix("fhir-definitions") {
      pathEndOrSingleSlash {
        get {
          parameterMap { queryParams =>
            queryParams.get("q") match {
              case Some(v) =>
                DefinitionsQuery.withName(v) match {
                  case DefinitionsQuery.RESOURCE_TYPES => complete("")
                  case DefinitionsQuery.PROFILES => complete("profiles queried")
                }
              case None => complete("What to return?")
            }
          }
        }
      }
    }
}

object DefinitionsQuery extends Enumeration {
  type DefinitionsQuery = Value
  final val RESOURCE_TYPES = Value("rtypes")
  final val PROFILES = Value("profiles")
}
