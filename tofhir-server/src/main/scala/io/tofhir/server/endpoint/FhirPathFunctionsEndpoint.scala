package io.tofhir.server.endpoint

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.server.common.model.ToFhirRestCall
import io.tofhir.server.endpoint.FhirPathFunctionsEndpoint.SEGMENT_FHIR_PATH_FUNCTIONS
import io.tofhir.common.model.Json4sSupport._
import io.tofhir.server.service.fhir.FhirPathFunctionsService

/**
 * Endpoint to manage FhirPath functions.
 * */
class FhirPathFunctionsEndpoint extends LazyLogging {

  val service: FhirPathFunctionsService = new FhirPathFunctionsService()

  def route(request: ToFhirRestCall): Route = {
    pathPrefix(SEGMENT_FHIR_PATH_FUNCTIONS) {
      pathEndOrSingleSlash {
        getFhirPathFunctionsDocumentation
      }
    }
  }

  /**
   * Returns the documentations of FhirPath functions.
   * */
  private def getFhirPathFunctionsDocumentation: Route = {
    get {
      complete {
        service.getFhirPathFunctionsDocumentation
      }
    }
  }
}

object FhirPathFunctionsEndpoint {
  val SEGMENT_FHIR_PATH_FUNCTIONS = "fhir-path-functions"
}
