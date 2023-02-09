import java.io.File
import akka.http.scaladsl.model.{ContentTypes, HttpMethod, StatusCodes}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import io.onfhir.util.JsonFormatter.formats
import io.tofhir.engine.config.ToFhirEngineConfig
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.endpoint.ProjectEndpoint
import io.tofhir.server.model.{Project, ProjectEditableFields, ToFhirRestCall}
import io.tofhir.server.service.project.ProjectFolderRepository
import io.tofhir.server.util.FileOperations
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.Serialization.writePretty
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ProjectEndpointTest extends AnyWordSpec with Matchers with ScalatestRouteTest with BeforeAndAfterAll {

  // toFHIR engine config
  val toFhirEngineConfig: ToFhirEngineConfig = new ToFhirEngineConfig(system.settings.config.getConfig("tofhir"))
  // project endpoint to be tested
  val projectEndpoint: ProjectEndpoint = new ProjectEndpoint(toFhirEngineConfig)
  // route of project endpoint
  // it is initialized with dummy rest call
  val route: Route = projectEndpoint.route(new ToFhirRestCall(HttpMethod.custom("GET"), "", ""))

  // first project to be created
  val project1: Project = Project(name = "example", description = Some("example project"))
  // second project to be created
  val project2: Project = Project(name = "second example", description = Some("second example project"))
  // patch to be applied to the existing project
  val projectPatch = ProjectEditableFields.DESCRIPTION -> "updated description"

  // first project created
  var createdProject1: Project = _

  "The service" should {

    "create a project" in {
      // create the first project
      Post("/projects", akka.http.scaladsl.model.HttpEntity.apply(ContentTypes.`application/json`, writePretty(project1))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        val project: Project = JsonMethods.parse(responseAs[String]).extract[Project]
        // set the created project
        createdProject1 = project
        // validate that projects metadata file is updated
        val projects = FileOperations.readJsonContent[Project](new File(toFhirEngineConfig.repositoryRootPath + File.separatorChar + ProjectFolderRepository.PROJECTS_JSON))
        projects.length shouldEqual 1
      }
      // create the second project
      Post("/projects", akka.http.scaladsl.model.HttpEntity.apply(ContentTypes.`application/json`, writePretty(project2))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that projects metadata file is updated
        val projects = FileOperations.readJsonContent[Project](new File(toFhirEngineConfig.repositoryRootPath + File.separatorChar + ProjectFolderRepository.PROJECTS_JSON))
        projects.length shouldEqual 2
      }
    }

    "get all projects" in {
      // retrieve all projects
      Get("/projects") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that it returns two projects
        val projects: Seq[Project] = JsonMethods.parse(responseAs[String]).extract[Seq[Project]]
        projects.length shouldEqual 2
      }
    }

    "get a project" in {
      // get a project
      Get(s"/projects/${createdProject1.id}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate the retrieved project
        val project: Project = JsonMethods.parse(responseAs[String]).extract[Project]
        project.id shouldEqual createdProject1.id
        project.name shouldEqual createdProject1.name
      }
    }

    "patch a project" in {
      // patch a project
      Patch(s"/projects/${createdProject1.id}", akka.http.scaladsl.model.HttpEntity.apply(ContentTypes.`application/json`, writePretty(projectPatch))) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that the returned project includes the update
        val project: Project = JsonMethods.parse(responseAs[String]).extract[Project]
        project.description.get should not equal project1.description.get
        // validate that projects metadata is updated
        val projects = FileOperations.readJsonContent[Project](new File(toFhirEngineConfig.repositoryRootPath + File.separatorChar + ProjectFolderRepository.PROJECTS_JSON))
        projects.find(p => p.id.contentEquals(createdProject1.id)).get.description.get shouldEqual "updated description"
      }
    }

    "delete a project" in {
      // delete a project
      Delete(s"/projects/${createdProject1.id}") ~> route ~> check {
        status shouldEqual StatusCodes.NoContent
        // validate that projects metadata file is updated
        val projects = FileOperations.readJsonContent[Project](new File(toFhirEngineConfig.repositoryRootPath + File.separatorChar + ProjectFolderRepository.PROJECTS_JSON))
        projects.length shouldEqual 1
      }
      // delete a non-existent project
      Delete(s"/projects/${createdProject1.id}") ~> route ~> check {
        status should not equal StatusCodes.NoContent
      }
    }
  }

  /**
   * Creates a repository folder before tests are run.
   * */
  override def beforeAll(): Unit = {
    new File(toFhirEngineConfig.repositoryRootPath).mkdir()
  }

  /**
   * Deletes the repository folder after all test cases are completed.
   * */
  override def afterAll(): Unit = {
    org.apache.commons.io.FileUtils.deleteDirectory(new File(toFhirEngineConfig.repositoryRootPath))
  }
}
