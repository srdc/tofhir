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
import io.tofhir.server.service.project.IProjectRepository
import io.tofhir.server.service.schema.ISchemaRepository

class SchemaDefinitionEndpoint(toFhirEngineConfig: ToFhirEngineConfig, schemaRepository: ISchemaRepository, projectRepository: IProjectRepository) extends LazyLogging {

  val service: SchemaDefinitionService = new SchemaDefinitionService(toFhirEngineConfig.schemaRepositoryFolderPath, schemaRepository, projectRepository)

  def route(request: ToFhirRestCall): Route = {
    pathPrefix(SEGMENT_SCHEMAS) {
      val projectId: String = request.projectId.get
      pathEndOrSingleSlash { // Operations on all schemas
        getAllSchemas(request) ~ createSchema(projectId)
      } ~ // Operations on a single schema identified by its id
        pathPrefix(Segment) { id: String =>
          getSchema(projectId, id) ~ updateSchema(projectId, id) ~ deleteSchema(projectId, id)
        }
    }
  }

  private def getAllSchemas(request: ToFhirRestCall): Route = {
    get {
      complete(service.getAllSchemas(request.projectId.get))
    }
  }

  private def createSchema(projectId: String): Route = {
    post { // Create a new schema definition
      entity(as[SchemaDefinition]) { schemaDefinition =>
        complete {
          service.createSchema(projectId, schemaDefinition) map { createdDefinition =>
            StatusCodes.Created -> createdDefinition
          }
        }
      }
    }
  }

  private def getSchema(projectId: String, id: String): Route = {
    get {
      complete {
        service.getSchema(projectId, id) map {
          case Some(schemaDefinition) => StatusCodes.OK -> schemaDefinition
          case None => StatusCodes.NotFound -> s"Schema definition with name $id not found"
        }
      }
    }
  }

  private def updateSchema(projectId: String, id: String): Route = {
    put {
      entity(as[SchemaDefinition]) { schemaDefinition =>
        complete {
          service.putSchema(projectId, id, schemaDefinition) map { _ =>
            StatusCodes.OK -> schemaDefinition
          }
        }
      }
    }
  }

  private def deleteSchema(projectId: String, id: String): Route = {
    delete {
      complete {
        service.deleteSchema(projectId, id) map { _ =>
          StatusCodes.NoContent
        }
      }
    }
  }

}

object SchemaDefinitionEndpoint {
  val SEGMENT_SCHEMAS = "schemas"
}

