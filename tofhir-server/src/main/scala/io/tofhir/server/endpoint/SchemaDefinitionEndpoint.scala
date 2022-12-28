package io.tofhir.server.endpoint

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.config.ToFhirEngineConfig
import io.tofhir.server.endpoint.SchemaDefinitionEndpoint.SEGMENT_SCHEMAS
import io.tofhir.server.model.{SimpleStructureDefinition, ToFhirRestCall}
import io.tofhir.server.service.SchemaDefinitionService
import io.tofhir.server.model.Json4sSupport._

import io.tofhir.engine.Execution.actorSystem.dispatcher

class SchemaDefinitionEndpoint(toFhirEngineConfig: ToFhirEngineConfig) extends LazyLogging {

  val service: SchemaDefinitionService = new SchemaDefinitionService(toFhirEngineConfig.schemaRepositoryFolderPath)

  def route(request: ToFhirRestCall): Route = {
    pathPrefix(SEGMENT_SCHEMAS) {
      pathEndOrSingleSlash {
        get { // Retrieve all schema definitions
          complete(service.getAllSchemaDefinitions)
        } ~
          post { // Create a new schema definition
            entity(as[SimpleStructureDefinition]) { simpleStructureDefinition =>
              complete {
                service.createSchema(simpleStructureDefinition) map { createdDefinition =>
                  StatusCodes.Created -> createdDefinition
                }
              }
            }
          }
      }
    }
  }

}

object SchemaDefinitionEndpoint {
  val SEGMENT_SCHEMAS = "schema"
}

