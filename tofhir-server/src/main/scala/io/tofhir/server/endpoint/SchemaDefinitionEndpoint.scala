package io.tofhir.server.endpoint

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.config.ToFhirEngineConfig
import io.tofhir.server.endpoint.SchemaDefinitionEndpoint.SEGMENT_SCHEMAS
import io.tofhir.server.model.ToFhirRestCall
import io.tofhir.server.service.SchemaDefinitionService

import io.tofhir.server.model.Json4sSupport._

class SchemaDefinitionEndpoint(toFhirEngineConfig: ToFhirEngineConfig) extends LazyLogging {

  val service: SchemaDefinitionService = new SchemaDefinitionService(toFhirEngineConfig.schemaRepositoryFolderPath)

  def route(request: ToFhirRestCall): Route = {
    pathPrefix(SEGMENT_SCHEMAS) {
      pathEndOrSingleSlash {
        get { // Retrieve all schema definitions
          complete(service.getAllSchemaDefinitions)
        }
      }
    }
  }

}

object SchemaDefinitionEndpoint {
  val SEGMENT_SCHEMAS = "schema"
}

