package io.tofhir.server.endpoint

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.server.endpoint.SchemaDefinitionEndpoint.SEGMENT_SCHEMAS
import io.tofhir.server.model.ToFhirRestCall

class SchemaDefinitionEndpoint extends LazyLogging {
  def route(request: ToFhirRestCall): Route =
    pathPrefix(SEGMENT_SCHEMAS) {
      pathEndOrSingleSlash {
        createOrUploadSchemaDefinitionRoute(request) ~ searchSchemaDefinitionRoute(request)
      } ~
        // /structure-definitions/{definitionId}
        // operations on a single structure definition
        pathPrefix(Segment) { schemaId: String =>
          pathEndOrSingleSlash {
            null
            //retrieveStructureDefinitionRoute(request, definitionId) ~ patchStructureDefinitionRoute(request, definitionId) ~ deleteStructureDefinitionRoute(request, definitionId)
          } ~
            null
//            activateStructureDefinitionRoute(request, definitionId) ~
//            elementDefinitionRoute(request, definitionId)
        }
    }

  private def createOrUploadSchemaDefinitionRoute(request: ToFhirRestCall): Route = {
    post {
      extractRequestEntity { httpEntity =>
        null
      }
    }
  }

  private def searchSchemaDefinitionRoute(request: ToFhirRestCall): Route = {
    get {
      parameterMap { queryParams =>
        val msg = s"Searching the schema definitions. HttpMethod:${request.method.value}, Uri:${request.uri.toString()}, requestId:${request.requestId}"
        logger.info(msg)
        complete(StatusCodes.OK -> "Hello World toFHIR-Server! -- " + msg)
//        handleRequest[QueryMetaEntityRequest, AdminResponse](
//          request,
//          QueryMetaEntityRequest(request.requestId, request.realmId.getOrElse("*"), CommonResourceTypes.STRUCTURE_DEFINITION, queryParams),
//          structureDefinitionService.queryStructureDefinitions,
//          authorizationHandler
//        )
      }
    }
  }
}

object SchemaDefinitionEndpoint {
  val SEGMENT_SCHEMAS = "schema"
}

