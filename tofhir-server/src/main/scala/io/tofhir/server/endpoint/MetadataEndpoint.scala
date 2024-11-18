package io.tofhir.server.endpoint

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.onfhir.definitions.common.model.Json4sSupport._
import io.tofhir.engine.config.ToFhirEngineConfig
import io.tofhir.server.common.config.WebServerConfig
import io.tofhir.server.common.model.ToFhirRestCall
import io.tofhir.server.config.RedCapServiceConfig
import io.tofhir.server.endpoint.MetadataEndpoint.SEGMENT_METADATA
import io.onfhir.definitions.resource.fhir.FhirDefinitionsConfig
import io.tofhir.server.service.MetadataService

/**
 * Endpoint to return metadata of the server.
 * */
class MetadataEndpoint(toFhirEngineConfig: ToFhirEngineConfig,
                       webServerConfig: WebServerConfig,
                       fhirDefinitionsConfig: FhirDefinitionsConfig,
                       redCapServiceConfig: Option[RedCapServiceConfig]) extends LazyLogging {

  val service: MetadataService = new MetadataService(
    toFhirEngineConfig,
    webServerConfig,
    fhirDefinitionsConfig,
    redCapServiceConfig
  )

  def route(request: ToFhirRestCall): Route = {
    pathPrefix(SEGMENT_METADATA) {
      pathEndOrSingleSlash {
        getMetadata
      }
    }
  }

  /**
   * Returns the documentations of FhirPath functions.
   * */
  private def getMetadata: Route = {
    get {
      complete {
        service.getMetadata
      }
    }
  }
}

object MetadataEndpoint {
  val SEGMENT_METADATA = "metadata"
}
