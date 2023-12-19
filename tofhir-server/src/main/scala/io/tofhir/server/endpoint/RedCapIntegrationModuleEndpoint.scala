package io.tofhir.server.endpoint

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.server.common.model.ToFhirRestCall
import io.tofhir.server.endpoint.RedCapIntegrationModuleEndpoint.SEGMENT_REDCAP
import io.tofhir.server.model.Json4sSupport._
import io.tofhir.server.model.RedCapConfig
import io.tofhir.server.service.RedCapIntegrationModuleService

class RedCapIntegrationModuleEndpoint() extends LazyLogging {

  private val redcapService: RedCapIntegrationModuleService = new RedCapIntegrationModuleService()

  def route(request: ToFhirRestCall): Route = {
    pathPrefix(SEGMENT_REDCAP) {
      pathEndOrSingleSlash {
        startToFhirRedcap() ~ getToFhirRedcapConfig
      }
    }
  }

  /**
   * Route to start tofhir-redcap
   * */
  private def startToFhirRedcap(): Route = {
    post {
      entity(as[RedCapConfig]) { redCapConfig =>
        complete {
          redcapService.startToFhirRedcap(redCapConfig)
        }
      }
    }
  }

  /**
   * Route to retrieve tofhir-redcap configuration.
   * */
  private def getToFhirRedcapConfig: Route = {
    get {
      complete {
        redcapService.getToFhirRedcapConfig
      }
    }
  }
}

object RedCapIntegrationModuleEndpoint {
  val SEGMENT_REDCAP = "redcap"
}
