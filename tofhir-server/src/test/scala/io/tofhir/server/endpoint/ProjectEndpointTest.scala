package io.tofhir.server.endpoint

import akka.http.scaladsl.model.{ContentTypes, StatusCodes}
import io.onfhir.definitions.common.model.Json4sSupport.formats
import io.onfhir.definitions.common.model.SchemaDefinition
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.BaseEndpointTest
import io.tofhir.server.model.{Project, ProjectEditableFields}
import io.tofhir.server.util.TestUtil
import org.json4s.JArray
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.Serialization.writePretty

class ProjectEndpointTest extends BaseEndpointTest {
  // first project to be created
  val project1: Project = Project(name = "example", description = Some("example project"), schemaUrlPrefix = Some("https://example.com/StructureDefinition/"), mappingUrlPrefix = Some("https://example.com/mappings/"))
  // second project to be created
  val project2: Project = Project(name = "second example", description = Some("second example project"))
  // patch to be applied to the existing project
  val projectPatch: (String, String) = ProjectEditableFields.DESCRIPTION -> "updated description"
  // schema definition
  val schemaDefinition: SchemaDefinition = SchemaDefinition(id = "id",
    version = SchemaDefinition.VERSION_LATEST,
    url = "https://example.com/fhir/StructureDefinition/schema",
    `type` = "Ty",
    name = "name",
    description = Some("description"),
    rootDefinition = None,
    fieldDefinitions = None)

  "The service" should {

    "create a project" in {
      // create the first project
      // note that in the initialization of database, a dummy project is already created due to the schemas defined in test resources
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}", akka.http.scaladsl.model.HttpEntity.apply(ContentTypes.`application/json`, writePretty(project1))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that projects metadata file is updated
        val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
        projects.arr.length shouldEqual 1
      }
      // create the second project
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}", akka.http.scaladsl.model.HttpEntity.apply(ContentTypes.`application/json`, writePretty(project2))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that projects metadata file is updated
        val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
        projects.arr.length shouldEqual 2
      }
    }

    "get all projects" in {
      // retrieve all projects
      Get(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that it returns two projects
        val projects: Seq[Project] = JsonMethods.parse(responseAs[String]).extract[Seq[Project]]
        projects.length shouldEqual 2
      }
    }

    "get a project" in {
      // get a project
      Get(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/${project1.id}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate the retrieved project
        val project: Project = JsonMethods.parse(responseAs[String]).extract[Project]
        project.id shouldEqual project1.id
        project.name shouldEqual project1.name
        project.schemaUrlPrefix shouldEqual project1.schemaUrlPrefix
        project.mappingUrlPrefix shouldEqual project1.mappingUrlPrefix
      }
      // get a project with invalid id
      Get(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/123123") ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "patch a project" in {
      // patch a project
      Patch(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/${project1.id}", akka.http.scaladsl.model.HttpEntity.apply(ContentTypes.`application/json`, writePretty(projectPatch))) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that the returned project includes the update
        val project: Project = JsonMethods.parse(responseAs[String]).extract[Project]
        project.description.get should not equal project1.description.get
        // validate that projects metadata is updated
        val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
        (projects.arr.find(p => (p \ "id").extract[String] == project1.id).get \ "description").extract[String] shouldEqual "updated description"
      }
      // patch a project with invalid id
      Patch(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/123123", akka.http.scaladsl.model.HttpEntity.apply(ContentTypes.`application/json`, writePretty(projectPatch))) ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "delete a project" in {
      // first create a schema to trigger creation of the project folder under the schemas folder
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/${project1.id}/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}", akka.http.scaladsl.model.HttpEntity.apply(ContentTypes.`application/json`, writePretty(schemaDefinition))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that projects metadata file is updated for the first project
        val firstProject = TestUtil.getProjectJsonFile(toFhirEngineConfig).arr
          .find(p => (p \ "id").extract[String].contentEquals(project1.id)).get // find first project
        (firstProject \ "schemas").asInstanceOf[JArray].arr.length shouldEqual 1
        // validate the project folder has been created within the schemas
        FileUtils.getPath(toFhirEngineConfig.schemaRepositoryFolderPath, project1.id).toFile should exist
      }

      // delete a project
      Delete(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/${project1.id}") ~> route ~> check {
        status shouldEqual StatusCodes.NoContent
        // validate that projects metadata file is updated
        val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
        projects.arr.length shouldEqual 1

        // validate the project file has been deleted under the schemas folder
        FileUtils.getPath(toFhirEngineConfig.schemaRepositoryFolderPath, project1.id).toFile shouldNot exist
      }
      // delete a non-existent project
      Delete(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/${project1.id}") ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }
  }
}
