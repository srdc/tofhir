package io.tofhir.server

import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import io.tofhir.engine.config.ToFhirEngineConfig
import io.tofhir.server.config.WebServerConfig
import io.tofhir.server.endpoint.ToFhirServerEndpoint
import io.tofhir.server.fhir.FhirDefinitionsConfig
import io.tofhir.server.service.terminology.TerminologySystemFolderRepository
import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.File

trait BaseEndpointTest extends AnyWordSpec with Matchers with ScalatestRouteTest with BeforeAndAfterAll {
  // toFHIR engine config
  val toFhirEngineConfig: ToFhirEngineConfig = new ToFhirEngineConfig(system.settings.config.getConfig("tofhir"))
  val webServerConfig = new WebServerConfig(system.settings.config.getConfig("webserver"))
  val fhirDefinitionsConfig = new FhirDefinitionsConfig(system.settings.config.getConfig("fhir"))
  // route endpoint
  var route: Route = _

  /**
   * Create the folders and initialize the endpoint and route
   */
  override def beforeAll(): Unit = {
    // onfhir needs schema folder to be created in advance,
    // terminology, mapping and job folders are created automatically
    new File(toFhirEngineConfig.schemaRepositoryFolderPath).mkdirs()
    new File(fhirDefinitionsConfig.profilesPath.get).mkdirs()
    new File(fhirDefinitionsConfig.codesystemsPath.get).mkdirs()
    new File(fhirDefinitionsConfig.valuesetsPath.get).mkdirs()
    // initialize endpoint and route
    val endpoint = new ToFhirServerEndpoint(toFhirEngineConfig, webServerConfig, fhirDefinitionsConfig)
    route = endpoint.toFHIRRoute
  }

  /**
   * Deletes the repository folders after all test cases are completed.
   * */
  override def afterAll(): Unit = {
    FileUtils.deleteDirectory(new File(TerminologySystemFolderRepository.TERMINOLOGY_SYSTEMS_FOLDER))
    FileUtils.deleteDirectory(new File(toFhirEngineConfig.toFhirDbFolderPath))
    FileUtils.deleteDirectory(new File(toFhirEngineConfig.schemaRepositoryFolderPath))
    FileUtils.deleteDirectory(new File(toFhirEngineConfig.jobRepositoryFolderPath))
    FileUtils.deleteDirectory(new File(toFhirEngineConfig.mappingRepositoryFolderPath))
    FileUtils.deleteDirectory(new File(fhirDefinitionsConfig.profilesPath.get))
    FileUtils.deleteDirectory(new File(fhirDefinitionsConfig.codesystemsPath.get))
    FileUtils.deleteDirectory(new File(fhirDefinitionsConfig.valuesetsPath.get))
  }
}
