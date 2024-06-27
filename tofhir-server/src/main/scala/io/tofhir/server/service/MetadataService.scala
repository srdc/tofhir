package io.tofhir.server.service

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse}
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.unmarshalling.Unmarshal
import io.tofhir.engine.config.ToFhirEngineConfig
import io.tofhir.server.common.config.WebServerConfig
import io.tofhir.server.config.RedCapServiceConfig
import io.tofhir.server.fhir.FhirDefinitionsConfig
import io.tofhir.engine.Execution.actorSystem
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.server.endpoint.MetadataEndpoint.SEGMENT_METADATA
import io.tofhir.server.model.{Archiving, Metadata, RepositoryNames}

import java.util.Properties
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success, Try}

/**
 * Service for retrieving metadata about the toFHIR server.
 * @param toFhirEngineConfig engine related configurations
 * @param webServerConfig web server related configurations
 * @param fhirDefinitionsConfig fhir related configurations
 * @param redCapServiceConfig redcap service related configurations
 */
class MetadataService(toFhirEngineConfig: ToFhirEngineConfig,
                      webServerConfig: WebServerConfig,
                      fhirDefinitionsConfig: FhirDefinitionsConfig,
                      redCapServiceConfig: RedCapServiceConfig) {
  /**
   * Use configurations to create a Metadata object along with the version set in pom.xml.
   * @return
   */
  def getMetadata: Metadata = {
    val properties: Properties  = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("version.properties"))
    val toFhirRedCapVersion = getToFhirRedCapVersion
      Metadata(
        name = "toFHIR",
        description = "toFHIR is a tool for mapping data from various sources to FHIR resources.",
        version = properties.getProperty("application.version"),
        fhirDefinitionsVersion = fhirDefinitionsConfig.majorFhirVersion,
        toFhirRedcapVersion = toFhirRedCapVersion,
        definitionsRootUrls = fhirDefinitionsConfig.definitionsRootURLs,
        schemasFhirVersion = toFhirEngineConfig.schemaRepositoryFhirVersion,
        repositoryNames = RepositoryNames(
          mappings = toFhirEngineConfig.mappingRepositoryFolderPath,
          schemas = toFhirEngineConfig.schemaRepositoryFolderPath,
          contexts = toFhirEngineConfig.mappingContextRepositoryFolderPath,
          jobs = toFhirEngineConfig.jobRepositoryFolderPath,
          terminologySystems = toFhirEngineConfig.terminologySystemFolderPath
        ),
        archiving = Archiving(
          erroneousRecordsFolder = toFhirEngineConfig.erroneousRecordsFolder,
          archiveFolder = toFhirEngineConfig.archiveFolder,
          streamArchivingFrequency = toFhirEngineConfig.streamArchivingFrequency
        )
      )
  }

  /**
   * Try to connect to the tofhir-redcap service to get the version of the toFHIR redcap version.
   * If no response is received, return None.
   * @return
   */
  private def getToFhirRedCapVersion: Option[String] = {
    val proxiedRequest = HttpRequest(
      method = HttpMethods.GET,
      uri = s"${redCapServiceConfig.endpoint}/$SEGMENT_METADATA",
      headers = RawHeader("Content-Type", "application/json") :: Nil
    )

    val responseFuture: Future[HttpResponse] = Http().singleRequest(proxiedRequest)
    val responseAsString = Try(Await.result(
      responseFuture.flatMap(resp => Unmarshal(resp.entity).to[String]),
      1.seconds // increasing this leads to increase initial loading time of the toFHIR frontend
    ))

    responseAsString match {
      case Success(res) => Some(res)
      case Failure(_) => None
    }
  }


}