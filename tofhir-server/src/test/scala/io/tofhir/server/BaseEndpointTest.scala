package io.tofhir.server

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import io.onfhir.client.OnFhirNetworkClient
import io.tofhir.common.model.Json4sSupport.formats
import io.tofhir.engine.config.ToFhirEngineConfig
import io.tofhir.engine.util.FhirMappingJobFormatter.EnvironmentVariable
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.config.LogServiceConfig
import io.tofhir.server.common.config.WebServerConfig
import io.tofhir.server.endpoint.ToFhirServerEndpoint
import io.tofhir.server.fhir.FhirDefinitionsConfig
import io.tofhir.server.model.Project
import io.tofhir.server.service.project.ProjectFolderRepository
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.Serialization.writePretty
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.File
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

trait BaseEndpointTest extends AnyWordSpec with Matchers with ScalatestRouteTest with BeforeAndAfterAll {
  // toFHIR engine config
  val toFhirEngineConfig: ToFhirEngineConfig = new ToFhirEngineConfig(system.settings.config.getConfig("tofhir"))
  val webServerConfig = new WebServerConfig(system.settings.config.getConfig("webserver"))
  val fhirDefinitionsConfig = new FhirDefinitionsConfig(system.settings.config.getConfig("fhir"))
  val logServiceConfig = new LogServiceConfig(system.settings.config.getConfig("log-service"))
  // route endpoint
  var route: Route = _

  // URL for the FHIR repository, defaulting to a local onFHIR if the environment variable is not set
  val fhirRepoUrl: String = sys.env.getOrElse(EnvironmentVariable.FHIR_REPO_URL.toString, "http://localhost:8081/fhir")
  // Instance of OnFhirNetworkClient initialized with the FHIR repository URL
  private val onFhirClient: OnFhirNetworkClient = OnFhirNetworkClient.apply(fhirRepoUrl)
  // Boolean indicating whether the FHIR server is available
  val fhirServerIsAvailable: Boolean =
    Try(Await.result(onFhirClient.search("Patient").execute(), FiniteDuration(5, TimeUnit.SECONDS)).httpStatus == StatusCodes.OK)
      .getOrElse(false)

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
    // delete projects metadata file if exists
    val projectsJson: File = FileUtils.getPath(ProjectFolderRepository.PROJECTS_JSON).toFile
    if(projectsJson.exists()) {
      org.apache.commons.io.FileUtils.delete(projectsJson)
    }
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
