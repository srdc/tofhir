package io.tofhir.server

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import io.onfhir.client.OnFhirNetworkClient
import io.tofhir.common.model.Json4sSupport.formats
import io.tofhir.engine.config.ToFhirEngineConfig
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.config.RedCapServiceConfig
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
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.utility.DockerImageName

import java.io.File
import java.time.Duration
import java.util.UUID

trait BaseEndpointTest extends AnyWordSpec with Matchers with ScalatestRouteTest with BeforeAndAfterAll {
  // toFHIR engine config
  val toFhirEngineConfig: ToFhirEngineConfig = new ToFhirEngineConfig(system.settings.config.getConfig("tofhir"))
  val webServerConfig = new WebServerConfig(system.settings.config.getConfig("webserver"))
  val fhirDefinitionsConfig = new FhirDefinitionsConfig(system.settings.config.getConfig("fhir"))
  val redCapServiceConfig = new RedCapServiceConfig(system.settings.config.getConfig("tofhir-redcap"))
  // route endpoint
  var route: Route = _

  // Instance of OnFhirNetworkClient initialized with onFhir container
  lazy val onFhirClient: OnFhirNetworkClient = initializeOnFhirClient()

  /**
   * Deploy an onFhir container for testing purpose
   * */
  def initializeOnFhirClient(): OnFhirNetworkClient = {
    @Container
    val container: GenericContainer[Nothing] = new GenericContainer(DockerImageName.parse("srdc/onfhir:r4")).withExposedPorts(8081);
    container.addEnv("DB_EMBEDDED", "true");
    container.addEnv("SERVER_PORT", "8081");
    container.addEnv("SERVER_BASE_URI", "fhir");
    container.addEnv("FHIR_ROOT_URL", s"http://${container.getHost}:8081/fhir")
    // The default startup timeout of 60 seconds is not sufficient for OnFhir, as it utilizes an embedded
    // MongoDB, which requires additional initialization time. Therefore, we increase the timeout to 3 minutes (180 seconds).
    container.waitingFor(Wait.forHttp("/fhir").forStatusCode(200).withStartupTimeout(Duration.ofSeconds(180)))
    container.start();
    OnFhirNetworkClient.apply(s"http://${container.getHost}:${container.getFirstMappedPort}/fhir");
  }
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
    val endpoint = new ToFhirServerEndpoint(toFhirEngineConfig, webServerConfig, fhirDefinitionsConfig, redCapServiceConfig)
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
