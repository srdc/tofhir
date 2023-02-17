package io.tofhir.server

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import io.onfhir.util.JsonFormatter.formats
import io.tofhir.engine.config.ToFhirEngineConfig
import io.tofhir.engine.model.FhirMapping
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.config.WebServerConfig
import io.tofhir.server.endpoint.ToFhirServerEndpoint
import io.tofhir.server.fhir.FhirDefinitionsConfig
import io.tofhir.server.model.{MappingMetadata, Project}
import io.tofhir.server.service.project.ProjectFolderRepository
import io.tofhir.server.util.FileOperations
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.Serialization.writePretty
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.File

class MappingEndpointTest extends AnyWordSpec with Matchers with ScalatestRouteTest with BeforeAndAfterAll {
  // toFHIR engine config
  val toFhirEngineConfig: ToFhirEngineConfig = new ToFhirEngineConfig(system.settings.config.getConfig("tofhir"))

  val webServerConfig = new WebServerConfig(system.settings.config.getConfig("webserver"))
  val fhirDefinitionsConfig = new FhirDefinitionsConfig(system.settings.config.getConfig("fhir"))
  var endpoint: ToFhirServerEndpoint = _
  // route endpoint
  var route: Route = _
  // first project to be created
  val project1: Project = Project(name = "example", description = Some("example project"))
  // first mapping to be created
  val mapping1: FhirMapping = FhirMapping(url = "http://example.com/mapping1", name = "mapping1", source = Seq.empty, context = Map.empty, mapping = Seq.empty)
  // second mapping to be created
  val mapping2: FhirMapping = FhirMapping(url = "http://example.com/mapping2", name = "mapping2", source = Seq.empty, context = Map.empty, mapping = Seq.empty)
  // first project created
  var createdProject1: Project = _

  "The service" should {

    "create a mapping within project" in {
      // create the first mapping
      Post(s"/${webServerConfig.baseUri}/projects/${createdProject1.id}/mapping", HttpEntity(ContentTypes.`application/json`, writePretty(mapping1))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that mapping metadata file is updated
        val projects = FileOperations.readJsonContent[Project](FileUtils.getPath(toFhirEngineConfig.toFhirDbFolderPath, ProjectFolderRepository.PROJECTS_JSON).toFile)
        projects.find(_.id == createdProject1.id).get.mappings.length shouldEqual 1
        // check mapping folder is created
        FileUtils.getPath(toFhirEngineConfig.mappingRepositoryFolderPath, createdProject1.id, mapping1.id).toFile.exists()
      }
      // create the second mapping
      Post(s"/${webServerConfig.baseUri}/projects/${createdProject1.id}/mapping", HttpEntity(ContentTypes.`application/json`, writePretty(mapping2))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that mapping metadata file is updated
        val projects = FileOperations.readJsonContent[Project](FileUtils.getPath(toFhirEngineConfig.toFhirDbFolderPath, ProjectFolderRepository.PROJECTS_JSON).toFile)
        projects.find(_.id == createdProject1.id).get.mappings.length shouldEqual 2
        FileUtils.getPath(toFhirEngineConfig.mappingRepositoryFolderPath, createdProject1.id, mapping2.id).toFile.exists()
      }
    }

    "get all mappings in a project" in {
      // get all mappings within a project
      Get(s"/${webServerConfig.baseUri}/projects/${createdProject1.id}/mapping") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that it returns two mappings
        val mappings: Seq[MappingMetadata] = JsonMethods.parse(responseAs[String]).extract[Seq[MappingMetadata]]
        mappings.length shouldEqual 2
      }
    }

    "get a mapping in a project" in {
      // get a mapping
      Get(s"/${webServerConfig.baseUri}/projects/${createdProject1.id}/mapping/${mapping1.id}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate the retrieved mapping
        val mapping: FhirMapping = JsonMethods.parse(responseAs[String]).extract[FhirMapping]
        mapping.url shouldEqual mapping1.url
        mapping.name shouldEqual mapping1.name
      }
      // get a mapping with invalid id
      Get(s"/${webServerConfig.baseUri}/projects/${createdProject1.id}/mapping/123123") ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "update a mapping in a project" in {
      // update a mapping
      Put(s"/${webServerConfig.baseUri}/projects/${createdProject1.id}/mapping/${mapping1.id}", HttpEntity(ContentTypes.`application/json`, writePretty(mapping1.copy(url = "http://example.com/mapping3")))) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that the returned mapping includes the update
        val mapping: FhirMapping = JsonMethods.parse(responseAs[String]).extract[FhirMapping]
        mapping.url shouldEqual "http://example.com/mapping3"
        // validate that mapping metadata is updated
        val projects = FileOperations.readJsonContent[Project](new File(toFhirEngineConfig.toFhirDbFolderPath + File.separatorChar + ProjectFolderRepository.PROJECTS_JSON))
        projects.find(_.id == createdProject1.id).get.mappings.find(_.id == mapping1.id).get.url shouldEqual "http://example.com/mapping3"
      }
      // update a mapping with invalid id
      Put(s"/${webServerConfig.baseUri}/projects/${createdProject1.id}/mapping/123123", HttpEntity(ContentTypes.`application/json`, writePretty(mapping1.copy(id = "123123")))) ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "delete a mapping from a project" in {
      // delete a mapping
      Delete(s"/${webServerConfig.baseUri}/projects/${createdProject1.id}/mapping/${mapping1.id}") ~> route ~> check {
        status shouldEqual StatusCodes.NoContent
        // validate that mapping metadata file is updated
        val projects = FileOperations.readJsonContent[Project](new File(toFhirEngineConfig.toFhirDbFolderPath + File.separatorChar + ProjectFolderRepository.PROJECTS_JSON))
        projects.find(_.id == createdProject1.id).get.mappings.length shouldEqual 1
        // check mapping folder is deleted
        FileUtils.getPath(toFhirEngineConfig.mappingRepositoryFolderPath, createdProject1.id, mapping1.id).toFile.exists() shouldEqual false
      }
      // delete a mapping with invalid id
      Delete(s"/${webServerConfig.baseUri}/projects/${createdProject1.id}/mapping/123123") ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

  }

  /**
   * Creates a repository folder before tests are run.
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
   * Deletes the repository folder after all test cases are completed.
   * */
  override def afterAll(): Unit = {
    org.apache.commons.io.FileUtils.deleteDirectory(new File(toFhirEngineConfig.toFhirDbFolderPath))
    org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(toFhirEngineConfig.mappingRepositoryFolderPath, createdProject1.id).toFile)
  }
}
