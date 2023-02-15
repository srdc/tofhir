package io.tofhir.server

import akka.http.scaladsl.model.{ContentTypes, StatusCodes}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import io.onfhir.util.JsonFormatter.formats
import io.tofhir.engine.config.ToFhirEngineConfig
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.config.WebServerConfig
import io.tofhir.server.endpoint.ToFhirServerEndpoint
import io.tofhir.server.fhir.FhirDefinitionsConfig
import io.tofhir.server.model.{Project, ProjectEditableFields, SchemaDefinition}
import io.tofhir.server.service.project.ProjectFolderRepository
import io.tofhir.server.util.FileOperations
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.Serialization.writePretty
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.File

class ProjectEndpointTest extends AnyWordSpec with Matchers with ScalatestRouteTest with BeforeAndAfterAll {
  // toFHIR engine config
  val toFhirEngineConfig: ToFhirEngineConfig = new ToFhirEngineConfig(system.settings.config.getConfig("tofhir"))

  val webServerConfig = new WebServerConfig(system.settings.config.getConfig("webserver"))
  val fhirDefinitionsConfig = new FhirDefinitionsConfig(system.settings.config.getConfig("fhir"))
  var endpoint:ToFhirServerEndpoint = _
  // route endpoint
  var route: Route = _

  // first project to be created
  val project1: Project = Project(name = "example", description = Some("example project"))
  // second project to be created
  val project2: Project = Project(name = "second example", description = Some("second example project"))
  // patch to be applied to the existing project
  val projectPatch = ProjectEditableFields.DESCRIPTION -> "updated description"
  // schema definition
  val schemaDefinition: SchemaDefinition = SchemaDefinition("id", "https://example.com/fhir/StructureDefinition/schema", "ty", "name", None, None)
  // first project created
  var createdProject1: Project = _

  "The service" should {

    "create a project" in {
      // create the first project
      // note that in the initialization of database, a dummy project is already created due to the schemas defined in test resources
      Post(s"/${webServerConfig.baseUri}/projects", akka.http.scaladsl.model.HttpEntity.apply(ContentTypes.`application/json`, writePretty(project1))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        val project: Project = JsonMethods.parse(responseAs[String]).extract[Project]
        // set the created project
        createdProject1 = project
        // validate that projects metadata file is updated
        val projects = FileOperations.readJsonContent[Project](new File(toFhirEngineConfig.repositoryRootPath + File.separatorChar + ProjectFolderRepository.PROJECTS_JSON))
        projects.length shouldEqual 2
      }
      // create the second project
      Post(s"/${webServerConfig.baseUri}/projects", akka.http.scaladsl.model.HttpEntity.apply(ContentTypes.`application/json`, writePretty(project2))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that projects metadata file is updated
        val projects = FileOperations.readJsonContent[Project](new File(toFhirEngineConfig.repositoryRootPath + File.separatorChar + ProjectFolderRepository.PROJECTS_JSON))
        projects.length shouldEqual 3
      }
    }

    "get all projects" in {
      // retrieve all projects
      Get(s"/${webServerConfig.baseUri}/projects") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that it returns two projects
        val projects: Seq[Project] = JsonMethods.parse(responseAs[String]).extract[Seq[Project]]
        projects.length shouldEqual 3
      }
    }

    "get a project" in {
      // get a project
      Get(s"/${webServerConfig.baseUri}/projects/${createdProject1.id}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate the retrieved project
        val project: Project = JsonMethods.parse(responseAs[String]).extract[Project]
        project.id shouldEqual createdProject1.id
        project.name shouldEqual createdProject1.name
      }
      // get a project with invalid id
      Get(s"/${webServerConfig.baseUri}/projects/123123") ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "patch a project" in {
      // patch a project
      Patch(s"/${webServerConfig.baseUri}/projects/${createdProject1.id}", akka.http.scaladsl.model.HttpEntity.apply(ContentTypes.`application/json`, writePretty(projectPatch))) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that the returned project includes the update
        val project: Project = JsonMethods.parse(responseAs[String]).extract[Project]
        project.description.get should not equal project1.description.get
        // validate that projects metadata is updated
        val projects = FileOperations.readJsonContent[Project](new File(toFhirEngineConfig.repositoryRootPath + File.separatorChar + ProjectFolderRepository.PROJECTS_JSON))
        projects.find(p => p.id.contentEquals(createdProject1.id)).get.description.get shouldEqual "updated description"
      }
      // patch a project with invalid id
      Patch(s"/${webServerConfig.baseUri}/projects/123123", akka.http.scaladsl.model.HttpEntity.apply(ContentTypes.`application/json`, writePretty(projectPatch))) ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "delete a project" in {
      // first create a schema to trigger creation of the project folder under the schemas folder
      Post(s"/${webServerConfig.baseUri}/projects/${createdProject1.id}/schemas", akka.http.scaladsl.model.HttpEntity.apply(ContentTypes.`application/json`, writePretty(schemaDefinition))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that projects metadata file is updated
        val projects = FileOperations.readJsonContent[Project](new File(toFhirEngineConfig.repositoryRootPath + File.separatorChar + ProjectFolderRepository.PROJECTS_JSON))
        val project: Project = projects.head
        project.schemas.length === 2
        // validate the project folder has been created within the schemas
        FileUtils.getPath(toFhirEngineConfig.schemaRepositoryFolderPath, createdProject1.id).toFile.exists() === true
      }

      // delete a project
      Delete(s"/${webServerConfig.baseUri}/projects/${createdProject1.id}") ~> route ~> check {
        status shouldEqual StatusCodes.NoContent
        // validate that projects metadata file is updated
        val projects = FileOperations.readJsonContent[Project](new File(toFhirEngineConfig.repositoryRootPath + File.separatorChar + ProjectFolderRepository.PROJECTS_JSON))
        projects.length shouldEqual 2

        // validate the project file has been deleted under the schemas folder
        FileUtils.getPath(toFhirEngineConfig.schemaRepositoryFolderPath, createdProject1.id).toFile.exists() === false
        FileUtils.getPath(toFhirEngineConfig.mappingJobFileContextPath, createdProject1.id).toFile.exists() === false
        FileUtils.getPath(toFhirEngineConfig.mappingRepositoryFolderPath, createdProject1.id).toFile.exists() === false
      }
      // delete a non-existent project
      Delete(s"/${webServerConfig.baseUri}/projects/${createdProject1.id}") ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }
  }

  /**
   * Creates a repository folder before tests are run and initializes endpoint and route.
   * */
  override def beforeAll(): Unit = {
    new File(toFhirEngineConfig.repositoryRootPath).mkdir()
    // initialize endpoint and route
    endpoint = new ToFhirServerEndpoint(toFhirEngineConfig, webServerConfig, fhirDefinitionsConfig)
    route = endpoint.toFHIRRoute
  }

  /**
   * Deletes the repository folder after all test cases are completed.
   * */
  override def afterAll(): Unit = {
    org.apache.commons.io.FileUtils.deleteDirectory(new File(toFhirEngineConfig.repositoryRootPath))
  }
}
