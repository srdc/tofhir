package io.tofhir.server.endpoint

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.config.ToFhirEngineConfig
import io.tofhir.server.endpoint.SchemaDefinitionEndpoint.SEGMENT_SCHEMAS
import io.tofhir.server.model.Json4sSupport._
import io.tofhir.server.model.{SchemaDefinition, ToFhirRestCall}
import io.tofhir.server.service.SchemaDefinitionService

class SchemaDefinitionEndpoint(toFhirEngineConfig: ToFhirEngineConfig) extends LazyLogging {

  val service: SchemaDefinitionService = new SchemaDefinitionService(toFhirEngineConfig.schemaRepositoryFolderPath)

  def route(request: ToFhirRestCall): Route = {
    pathPrefix(SEGMENT_SCHEMAS) {
      pathEndOrSingleSlash { // Operations on all schemas
        getAllSchemas(request) ~ createSchema()
      } ~ // Operations on a single schema identified by its id
        pathPrefix(Segment) { id: String =>
          getSchema(id) ~ updateSchema(id) ~ deleteSchema(id)
        }
    }
  }

  private def getAllSchemas(request: ToFhirRestCall): Route = {
    get {
      parameter("projectId") { projectId =>
        complete(service.getAllSchemas(projectId))
      }
    }
  }

  private def createSchema(): Route = {
    post { // Create a new schema definition
      entity(as[SchemaDefinition]) { schemaDefinition =>
        complete {
          service.createSchema(schemaDefinition) map { createdDefinition =>
            StatusCodes.Created -> createdDefinition
          }
        }
      }
    }
  }

  private def getSchema(id: String): Route = {
    get {
      complete {
        service.getSchema(id) map {
          case Some(schemaDefinition) => StatusCodes.OK -> schemaDefinition
          case None => StatusCodes.NotFound -> s"Schema definition with name $id not found"
        }
      }
    }
  }

  private def updateSchema(id: String): Route = {
    put {
      entity(as[SchemaDefinition]) { schemaDefinition =>
        complete {
          service.putSchema(id, schemaDefinition) map { _ =>
            StatusCodes.OK -> schemaDefinition
          }
        }
      }
    }
  }

  private def deleteSchema(id: String): Route = {
    delete {
      complete {
        service.deleteSchema(id) map { _ =>
          StatusCodes.OK
        }
      }
    }
  }

}

object SchemaDefinitionEndpoint {
  val SEGMENT_SCHEMAS = "schemas"
}

