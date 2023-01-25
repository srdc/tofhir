package io.tofhir.server.endpoint

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.config.ToFhirEngineConfig
import io.tofhir.server.endpoint.MappingEndpoint.SEGMENT_MAPPING
import io.tofhir.server.model.Json4sSupport._
import io.tofhir.server.model.ToFhirRestCall
import io.tofhir.server.service.MappingService

class MappingEndpoint(toFhirEngineConfig: ToFhirEngineConfig) extends LazyLogging {

  val service: MappingService = new MappingService(toFhirEngineConfig.mappingRepositoryFolderPath)

  def route(request: ToFhirRestCall): Route = {
    pathPrefix(SEGMENT_MAPPING) {
      pathEndOrSingleSlash { // Operations on all mappings
        get {
          complete {
            service.getAllMetadata(None)
          }
        }
      } ~
        pathPrefix(Segment) { directoryName: String =>
          pathEndOrSingleSlash { // Assumption: mappingName and file name are equal
            get { // return mappings only in that directory. eg. pilot1
              complete {
                service.getAllMetadata(Some(directoryName))
              }
            }
          } ~  pathPrefix(Segment) { mappingName: String => // Operations on a single mapping identified by its name
            get { // pilot1/surgery-details-mapping
              complete {
                service.getMappingByName(directoryName, mappingName) map {
                  case Some(fhirMapping) => StatusCodes.OK -> fhirMapping
                  case None => StatusCodes.NotFound -> s"Mapping definition with name $mappingName not found"
                }
              }
            }
          }
        }

    }
  }

}

object MappingEndpoint {
  val SEGMENT_MAPPING = "mapping"
}



