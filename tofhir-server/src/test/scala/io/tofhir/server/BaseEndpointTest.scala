package io.tofhir.server

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import io.onfhir.util.JsonFormatter.formats
import io.tofhir.engine.config.ToFhirEngineConfig
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.config.{LogServiceConfig, WebServerConfig}
import io.tofhir.server.endpoint.ToFhirServerEndpoint
import io.tofhir.server.fhir.FhirDefinitionsConfig
import io.tofhir.server.model.Project
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.Serialization.writePretty
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.UUID

trait BaseEndpointTest extends AnyWordSpec with Matchers with ScalatestRouteTest with BeforeAndAfterAll {
  // toFHIR engine config
  val toFhirEngineConfig: ToFhirEngineConfig = new ToFhirEngineConfig(system.settings.config.getConfig("tofhir"))
  val webServerConfig = new WebServerConfig(system.settings.config.getConfig("webserver"))
  val fhirDefinitionsConfig = new FhirDefinitionsConfig(system.settings.config.getConfig("fhir"))
  val logServiceConfig = new LogServiceConfig(system.settings.config.getConfig("log-service"))
  // route endpoint
  var route: Route = _

  /**
   * Identifier of test project which can be used in endpoint tests.
   * Endpoint tests, which require a test project, should call {@link createProject} method to create it
   * */
  var projectId: String = _

  /**
   * Creates a test project whose identifier is stored in {@link projectId}.
   * */
  def createProject(id: Option[String] = None): Unit = {
    val project1: Project = Project(id = id.getOrElse(UUID.randomUUID().toString), name = "example", url = "https://www.example.com", description = Some("example project"))
    // create a project
    Post("/tofhir/projects", HttpEntity(ContentTypes.`application/json`, writePretty(project1))) ~> route ~> check {
      status shouldEqual StatusCodes.Created
      val project: Project = JsonMethods.parse(responseAs[String]).extract[Project]
      // set the created project
      projectId = project.id
    }
  }

  /**
   * Create the folders and initialize the endpoint and route
   */
  override def beforeAll(): Unit = {
    // Deleting folders to start with a clean environment
    cleanFolders()

    // onfhir needs schema folder to be created in advance,
    // terminology, job folders are created automatically
    FileUtils.getPath(toFhirEngineConfig.schemaRepositoryFolderPath).toFile.mkdirs()
    // Folder for the mapping repository is also created manually, as the engine's mapping repository requires it during the initialization
    FileUtils.getPath(toFhirEngineConfig.mappingRepositoryFolderPath).toFile.mkdirs()
    FileUtils.getPath(fhirDefinitionsConfig.profilesPath.get).toFile.mkdirs()
    FileUtils.getPath(fhirDefinitionsConfig.codesystemsPath.get).toFile.mkdirs()
    FileUtils.getPath(fhirDefinitionsConfig.valuesetsPath.get).toFile.mkdirs()
    // initialize endpoint and route
    val endpoint = new ToFhirServerEndpoint(toFhirEngineConfig, webServerConfig, fhirDefinitionsConfig, logServiceConfig)
    route = endpoint.toFHIRRoute
  }

  /**
   * Deletes the repository folders after all test cases are completed.
   * */
  override def afterAll(): Unit = {
    cleanFolders()
  }

  private def cleanFolders(): Unit = {
    org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(toFhirEngineConfig.toFhirDbFolderPath).toFile)
    org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(toFhirEngineConfig.terminologySystemFolderPath).toFile)
    org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(toFhirEngineConfig.schemaRepositoryFolderPath).toFile)
    org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(toFhirEngineConfig.jobRepositoryFolderPath).toFile)
    org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(toFhirEngineConfig.mappingRepositoryFolderPath).toFile)
    org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(toFhirEngineConfig.mappingContextRepositoryFolderPath).toFile)
    org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(fhirDefinitionsConfig.profilesPath.get).toFile)
    org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(fhirDefinitionsConfig.codesystemsPath.get).toFile)
    org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(fhirDefinitionsConfig.valuesetsPath.get).toFile)
  }
}
