package io.tofhir.server.project

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import io.onfhir.util.JsonFormatter.formats
import io.tofhir.engine.model.FhirMapping
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.BaseEndpointTest
import io.tofhir.server.util.TestUtil
import org.json4s.JArray
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.Serialization.writePretty

class MappingEndpointTest extends BaseEndpointTest {
  // first mapping to be created
  val mapping1: FhirMapping = FhirMapping(id = "mapping1", url = "http://example.com/mapping1", name = "mapping1", source = Seq.empty, context = Map.empty, mapping = Seq.empty)
  // second mapping to be created
  val mapping2: FhirMapping = FhirMapping(id = "mapping2", url = "http://example.com/mapping2", name = "mapping2", source = Seq.empty, context = Map.empty, mapping = Seq.empty)

  "The service" should {

    "create a mapping within project" in {
      // create the first mapping
      Post(s"/${webServerConfig.baseUri}/projects/${projectId}/mappings", HttpEntity(ContentTypes.`application/json`, writePretty(mapping1))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that mapping metadata file is updated
        val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
        (projects.arr.find(p => (p \ "id").extract[String] == projectId).get \ "mappings").asInstanceOf[JArray].arr.length shouldEqual 1
        // check mapping folder is created
        FileUtils.getPath(toFhirEngineConfig.mappingRepositoryFolderPath, projectId, mapping1.id).toFile.exists()
      }
      // create the second mapping
      Post(s"/${webServerConfig.baseUri}/projects/${projectId}/mappings", HttpEntity(ContentTypes.`application/json`, writePretty(mapping2))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that mapping metadata file is updated
        val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
        (projects.arr.find(p => (p \ "id").extract[String] == projectId).get \ "mappings").asInstanceOf[JArray].arr.length shouldEqual 2
        FileUtils.getPath(toFhirEngineConfig.mappingRepositoryFolderPath, projectId, mapping2.id).toFile.exists()
      }
    }

    "get all mappings in a project" in {
      // get all mappings within a project
      Get(s"/${webServerConfig.baseUri}/projects/${projectId}/mappings") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that it returns two mappings
        val mappings: Seq[FhirMapping] = JsonMethods.parse(responseAs[String]).extract[Seq[FhirMapping]]
        mappings.length shouldEqual 2
      }
    }

    "get a mapping in a project" in {
      // get a mapping
      Get(s"/${webServerConfig.baseUri}/projects/${projectId}/mappings/${mapping1.id}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate the retrieved mapping
        val mapping: FhirMapping = JsonMethods.parse(responseAs[String]).extract[FhirMapping]
        mapping.url shouldEqual mapping1.url
        mapping.name shouldEqual mapping1.name
      }
      // get a mapping with invalid id
      Get(s"/${webServerConfig.baseUri}/projects/${projectId}/mappings/123123") ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "update a mapping in a project" in {
      // update a mapping
      Put(s"/${webServerConfig.baseUri}/projects/${projectId}/mappings/${mapping1.id}", HttpEntity(ContentTypes.`application/json`, writePretty(mapping1.copy(url = "http://example.com/mapping3")))) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that the returned mapping includes the update
        val mapping: FhirMapping = JsonMethods.parse(responseAs[String]).extract[FhirMapping]
        mapping.url shouldEqual "http://example.com/mapping3"
        // validate that mapping metadata is updated
        val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
        (
          (projects.arr.find(p => (p \ "id").extract[String] == projectId)
            .get \ "mappings").asInstanceOf[JArray].arr
            .find(m => (m \ "id").extract[String].contentEquals(mapping1.id)).get \ "url"
          )
          .extract[String] shouldEqual "http://example.com/mapping3"
      }
      // update a mapping with invalid id
      Put(s"/${webServerConfig.baseUri}/projects/${projectId}/mappings/123123", HttpEntity(ContentTypes.`application/json`, writePretty(mapping1.copy(id = "123123")))) ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "delete a mapping from a project" in {
      // delete a mapping
      Delete(s"/${webServerConfig.baseUri}/projects/${projectId}/mappings/${mapping1.id}") ~> route ~> check {
        status shouldEqual StatusCodes.NoContent
        // validate that mapping metadata file is updated
        val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
        (projects.arr.find(p => (p \ "id").extract[String] == projectId)
          .get \ "mappings").asInstanceOf[JArray].arr.length shouldEqual 1
        // check mapping folder is deleted
        FileUtils.getPath(toFhirEngineConfig.mappingRepositoryFolderPath, projectId, mapping1.id).toFile.exists() shouldEqual false
      }
      // delete a mapping with invalid id
      Delete(s"/${webServerConfig.baseUri}/projects/${projectId}/mappings/123123") ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

  }

  /**
   * Creates a project to be used in the tests
   * */
  override def beforeAll(): Unit = {
    super.beforeAll()
    this.createProject()
  }
}
