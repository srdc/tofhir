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
        get { // Search/(Retrieve all) schema metadata (only url, type and name)
          parameters(Symbol("url").as[String].?, Symbol("reload").as[Boolean] ? false) { (url, withReload) =>
            url match {
              case Some(schemaUrl) => complete(service.getSchemaDefinition(schemaUrl, withReload))
              case None => complete(service.getAllMetadata(withReload))
            }
          }
        } ~
          post { // Create a new schema definition
            entity(as[SchemaDefinition]) { schemaDefinition =>
              complete {
                service.createSchema(schemaDefinition) map { createdDefinition =>
                  StatusCodes.Created -> createdDefinition
                }
              }
            }
          }
      } ~
        pathPrefix(Segment) { rootPath: String => // Operations on a single schema identified by its rootPath/type
          // Assumption: type and rootPath are equal
          get {
            complete {
              s"This is the rootPath:$rootPath"
            }
          } ~ post {
            null
          } ~ put {
            null
          }
        }
    }
  }

}

object SchemaDefinitionEndpoint {
  val SEGMENT_SCHEMAS = "schema"

  val QUERY_PARAM_URL = "url"
  val QUERY_PARAM_RELOAD = "reload" // Should be called as reload=true
}

