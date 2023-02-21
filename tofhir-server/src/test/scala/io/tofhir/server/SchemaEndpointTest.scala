package io.tofhir.server

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import io.onfhir.util.JsonFormatter.formats
import io.tofhir.engine.config.ToFhirEngineConfig
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.config.WebServerConfig
import io.tofhir.server.endpoint.ToFhirServerEndpoint
import io.tofhir.server.fhir.FhirDefinitionsConfig
import io.tofhir.server.model.{Project, SchemaDefinition}
import io.tofhir.server.service.project.ProjectFolderRepository
import io.tofhir.server.util.FileOperations
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.Serialization.writePretty
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.File

class SchemaEndpointTest extends AnyWordSpec with Matchers with ScalatestRouteTest with BeforeAndAfterAll {
  // toFHIR engine config
  val toFhirEngineConfig: ToFhirEngineConfig = new ToFhirEngineConfig(system.settings.config.getConfig("tofhir"))

  val webServerConfig = new WebServerConfig(system.settings.config.getConfig("webserver"))
  val fhirDefinitionsConfig = new FhirDefinitionsConfig(system.settings.config.getConfig("fhir"))
  var endpoint:ToFhirServerEndpoint = _
  // route endpoint
  var route: Route = _
  // first project to be created
  val project1: Project = Project(name = "example", description = Some("example project"))
  // first schema schema to be created
  val schema1: SchemaDefinition = SchemaDefinition(url = "https://example.com/fhir/StructureDefinition/schema", `type` = "ty", name = "name", rootDefinition = None, fieldDefinitions = None)
  // second schema to be created
  val schema2: SchemaDefinition = SchemaDefinition(url = "https://example.com/fhir/StructureDefinition/schema2", `type` = "ty2", name = "name2", rootDefinition = None, fieldDefinitions = None)
  // first project created
  var createdProject1: Project = _

  "The service" should {

    "create a schema within project" in {
      // create the first schema
      Post(s"/tofhir/projects/${createdProject1.id}/schemas", HttpEntity(ContentTypes.`application/json`, writePretty(schema1))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that schema metadata file is updated
        val projects = FileOperations.readJsonContent[Project](new File(toFhirEngineConfig.toFhirDbFolderPath + File.separatorChar + ProjectFolderRepository.PROJECTS_JSON))
        projects.find(_.id == createdProject1.id).get.schemas.length shouldEqual 1
        // check schema folder is created
        FileUtils.getPath(toFhirEngineConfig.schemaRepositoryFolderPath, createdProject1.id, schema1.id).toFile.exists()
//        FileUtils.exists(toFhirEngineConfig.toFhirDbFolderPath + File.separatorChar + createdProject1.id + File.separatorChar + "schemas" + File.separatorChar + schema1.id) shouldEqual true
      }
      // create the second schema
      Post(s"/tofhir/projects/${createdProject1.id}/schemas", HttpEntity(ContentTypes.`application/json`, writePretty(schema2))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that schema metadata file is updated
        val projects = FileOperations.readJsonContent[Project](new File(toFhirEngineConfig.toFhirDbFolderPath + File.separatorChar + ProjectFolderRepository.PROJECTS_JSON))
        projects.find(_.id == createdProject1.id).get.schemas.length shouldEqual 2
        FileUtils.getPath(toFhirEngineConfig.schemaRepositoryFolderPath, createdProject1.id, schema2.id).toFile.exists()
      }
    }

    "get all schemas in a project" in {
      // get all schemas within a project
      Get(s"/tofhir/projects/${createdProject1.id}/schemas") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that it returns two schemas
        val schemas: Seq[SchemaDefinition] = JsonMethods.parse(responseAs[String]).extract[Seq[SchemaDefinition]]
        schemas.length shouldEqual 2
      }
    }

    "get a schema in a project" in {
      // get a schema
      Get(s"/tofhir/projects/${createdProject1.id}/schemas/${schema1.id}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate the retrieved schema
        val schema: SchemaDefinition = JsonMethods.parse(responseAs[String]).extract[SchemaDefinition]
        schema.url shouldEqual schema1.url
        schema.`type` shouldEqual schema1.`type`
      }
      // get a schema with invalid id
      Get(s"/tofhir/projects/${createdProject1.id}/schemas/123123") ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "get a schema by url in a project" in {
      Get(s"/tofhir/projects/${createdProject1.id}/schemas?url=${schema1.url}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate the retrieved schema
        val schema: SchemaDefinition = JsonMethods.parse(responseAs[String]).extract[SchemaDefinition]
        schema.url shouldEqual schema1.url
        schema.name shouldEqual schema1.name
      }
    }

    "update a schema in a project" in {
      // update a schema
      Put(s"/tofhir/projects/${createdProject1.id}/schemas/${schema1.id}", HttpEntity(ContentTypes.`application/json`, writePretty(schema1.copy(url = "https://example.com/fhir/StructureDefinition/schema3")))) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that the returned schema includes the update
        val schema: SchemaDefinition = JsonMethods.parse(responseAs[String]).extract[SchemaDefinition]
        schema.url shouldEqual "https://example.com/fhir/StructureDefinition/schema3"
        // validate that schema metadata is updated
        val projects = FileOperations.readJsonContent[Project](new File(toFhirEngineConfig.toFhirDbFolderPath + File.separatorChar + ProjectFolderRepository.PROJECTS_JSON))
        projects.find(_.id == createdProject1.id).get.schemas.find(_.id == schema1.id).get.url shouldEqual "https://example.com/fhir/StructureDefinition/schema3"
      }
      // update a schema with invalid id
      Put(s"/tofhir/projects/${createdProject1.id}/schemas/123123", HttpEntity(ContentTypes.`application/json`, writePretty(schema2))) ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "delete a schema from a project" in {
      // delete a schema
      Delete(s"/tofhir/projects/${createdProject1.id}/schemas/${schema1.id}") ~> route ~> check {
        status shouldEqual StatusCodes.NoContent
        // validate that schema metadata file is updated
        val projects = FileOperations.readJsonContent[Project](new File(toFhirEngineConfig.toFhirDbFolderPath + File.separatorChar + ProjectFolderRepository.PROJECTS_JSON))
        projects.find(_.id == createdProject1.id).get.schemas.length shouldEqual 1
        // check schema folder is deleted
        FileUtils.getPath(toFhirEngineConfig.schemaRepositoryFolderPath, createdProject1.id, schema1.id).toFile.exists() shouldEqual false
      }
      // delete a schema with invalid id
      Delete(s"/tofhir/projects/${createdProject1.id}/schemas/123123") ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

  }

  /**
   * Creates a repository folder before tests are run and initializes endpoint and route.
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
    org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(toFhirEngineConfig.schemaRepositoryFolderPath, createdProject1.id).toFile)
  }
}
