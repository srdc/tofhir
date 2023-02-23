package io.tofhir.server

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, Multipart, StatusCodes}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import io.tofhir.engine.config.ToFhirEngineConfig
import io.tofhir.server.config.WebServerConfig
import io.tofhir.server.endpoint.ToFhirServerEndpoint
import io.tofhir.server.fhir.FhirDefinitionsConfig
import io.tofhir.server.model.Project
import org.json4s.jackson.Serialization.writePretty
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import io.onfhir.util.JsonFormatter.formats
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.service.project.ProjectFolderRepository
import io.tofhir.server.util.FileOperations
import org.json4s.jackson.JsonMethods

import java.io.File

class MappingContextEndpointTest extends AnyWordSpec with Matchers with ScalatestRouteTest with BeforeAndAfterAll {
  // toFHIR engine config
  val toFhirEngineConfig: ToFhirEngineConfig = new ToFhirEngineConfig(system.settings.config.getConfig("tofhir"))

  val webServerConfig = new WebServerConfig(system.settings.config.getConfig("webserver"))
  val fhirDefinitionsConfig = new FhirDefinitionsConfig(system.settings.config.getConfig("fhir"))
  var endpoint: ToFhirServerEndpoint = _
  // route endpoint
  var route: Route = _
  // first project to be created
  val project1: Project = Project(name = "example", description = Some("example project"))
  // first mapping context to be created
  val mappingContext1: String = "mappingContext1"
  // second mapping context to be created
  val mappingContext2: String = "mappingContext2"
  // first project created
  var createdProject1: Project = _

  /**
   * Creates a project folder before tests are run.
   * */
  override def beforeAll(): Unit = {
    new File(toFhirEngineConfig.toFhirDbFolderPath).mkdir()
    // initialize endpoint and route
    endpoint = new ToFhirServerEndpoint(toFhirEngineConfig, webServerConfig, fhirDefinitionsConfig)
    route = endpoint.toFHIRRoute
    // create a project
    Post("/tofhir/projects", HttpEntity(ContentTypes.`application/json`, writePretty(project1))) ~> route ~> check {
      status shouldEqual StatusCodes.Created
      val project: Project = JsonMethods.parse(responseAs[String]).extract[Project]
      // set the created project
      createdProject1 = project
    }
  }

  /**
   * Deletes the project folder after all test cases are completed.
   * */
  override def afterAll(): Unit = {
    org.apache.commons.io.FileUtils.deleteDirectory(new File(toFhirEngineConfig.toFhirDbFolderPath))
    org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(toFhirEngineConfig.mappingContextRepositoryFolderPath, createdProject1.id).toFile)
  }

  "The service" should {

    "create a mapping context within project" in {
      // create the first mapping context
      Post(s"/${webServerConfig.baseUri}/projects/${createdProject1.id}/mapping-contexts", HttpEntity(ContentTypes.`text/plain(UTF-8)`, mappingContext1)) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that mapping context is updated in projects.json file
        val projects = FileOperations.readJsonContent[Project](FileUtils.getPath(toFhirEngineConfig.toFhirDbFolderPath, ProjectFolderRepository.PROJECTS_JSON).toFile)
        projects.find(_.id == createdProject1.id).get.mappingContexts.length shouldEqual 1
        // check mapping context file is created
        FileUtils.getPath(toFhirEngineConfig.mappingContextRepositoryFolderPath, createdProject1.id, mappingContext1).toFile.exists()
      }
      // create the second mapping context
      Post(s"/${webServerConfig.baseUri}/projects/${createdProject1.id}/mapping-contexts", HttpEntity(ContentTypes.`text/plain(UTF-8)`, mappingContext2)) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that mapping context is updated in projects.json file
        val projects = FileOperations.readJsonContent[Project](FileUtils.getPath(toFhirEngineConfig.toFhirDbFolderPath, ProjectFolderRepository.PROJECTS_JSON).toFile)
        projects.find(_.id == createdProject1.id).get.mappingContexts.length shouldEqual 2
        FileUtils.getPath(toFhirEngineConfig.mappingContextRepositoryFolderPath, createdProject1.id, mappingContext2).toFile.exists()
      }
    }

    "get all mapping contexts within a project" in {
      Get(s"/${webServerConfig.baseUri}/projects/${createdProject1.id}/mapping-contexts") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        val mappingContexts = JsonMethods.parse(responseAs[String]).extract[List[String]]
        mappingContexts.length shouldEqual 2
        mappingContexts.contains(mappingContext1) shouldEqual true
        mappingContexts.contains(mappingContext2) shouldEqual true
      }
    }

    "delete a mapping context within a project" in {
      Delete(s"/${webServerConfig.baseUri}/projects/${createdProject1.id}/mapping-contexts/$mappingContext2") ~> route ~> check {
        status shouldEqual StatusCodes.NoContent
        // validate that mapping context is updated in projects.json file
        val projects = FileOperations.readJsonContent[Project](FileUtils.getPath(toFhirEngineConfig.toFhirDbFolderPath, ProjectFolderRepository.PROJECTS_JSON).toFile)
        projects.find(_.id == createdProject1.id).get.mappingContexts.length shouldEqual 1
        // check mapping context file is deleted
        FileUtils.getPath(toFhirEngineConfig.mappingContextRepositoryFolderPath, createdProject1.id, mappingContext2).toFile.exists() shouldEqual false
      }
    }

    "upload a csv content to a mapping context" in {
      // get file from resources
      val file: File = FileOperations.getFileIfExists(getClass.getResource("/sample-unit-conversion.csv").getPath)
      val fileData = Multipart.FormData.BodyPart.fromPath("attachment", ContentTypes.`text/plain(UTF-8)`, file.toPath)
      val formData = Multipart.FormData(fileData)
      // save a csv file to mapping context
      Post(s"/${webServerConfig.baseUri}/projects/${createdProject1.id}/mapping-contexts/$mappingContext1/content", formData.toEntity()) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[String] shouldEqual "OK"
        Thread.sleep(5000)
      }
    }

    "download csv content of a mapping context" in {
      // download csv content of the mapping context
      Get(s"/${webServerConfig.baseUri}/projects/${createdProject1.id}/mapping-contexts/$mappingContext1/content") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that it returns the csv content
        val csvContent: String = responseAs[String]
        // remove new lines to compare without system specific line endings
        csvContent.replaceAll("\\r", "").replaceAll("\\n", "") shouldEqual
          "source_code,source_unit,target_unit,conversion_function" +
          "10839-9,\"ng/ml\",\"ng/mL\",\"$this\"" +
          "14627-4,\"mmol/L\",\"mmol/L\",\"$this\"" +
          "1742-6,\"UI/L\",\"U/L\",\"$this\"" +
          "1920-8,\"UI/L\",\"U/L\",\"$this\""
      }
    }
  }

}
