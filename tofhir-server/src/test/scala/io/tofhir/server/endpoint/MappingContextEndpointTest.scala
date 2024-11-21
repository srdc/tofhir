package io.tofhir.server.endpoint

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, Multipart, StatusCodes}
import io.onfhir.definitions.common.model.Json4sSupport.formats
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.BaseEndpointTest
import io.tofhir.server.endpoint.{MappingContextEndpoint, ProjectEndpoint}
import io.tofhir.server.util.{FileOperations, TestUtil}
import org.json4s.JArray
import org.json4s.jackson.JsonMethods

import java.io.File

class MappingContextEndpointTest extends BaseEndpointTest {
  // first mapping context to be created
  val mappingContext1: String = "mappingContext1"
  // second mapping context to be created
  val mappingContext2: String = "mappingContext2"

  "The service" should {

    "create a mapping context within project" in {
      // create the first mapping context
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${MappingContextEndpoint.SEGMENT_CONTEXTS}", HttpEntity(ContentTypes.`text/plain(UTF-8)`, mappingContext1)) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that mapping context is updated in projects.json file
        val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
        (projects.arr.find(p => (p \ "id").extract[String] == projectId).get \ "mappingContexts").asInstanceOf[JArray].arr.length shouldEqual 1
        // check mapping context file is created
        FileUtils.getPath(toFhirEngineConfig.mappingContextRepositoryFolderPath, projectId, mappingContext1).toFile should exist
      }
      // create the second mapping context
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${MappingContextEndpoint.SEGMENT_CONTEXTS}", HttpEntity(ContentTypes.`text/plain(UTF-8)`, mappingContext2)) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that mapping context is updated in projects.json file
        val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
        (projects.arr.find(p => (p \ "id").extract[String] == projectId).get \ "mappingContexts").asInstanceOf[JArray].arr.length shouldEqual 2
        FileUtils.getPath(toFhirEngineConfig.mappingContextRepositoryFolderPath, projectId, mappingContext2).toFile should exist
      }
    }

    "get all mapping contexts within a project" in {
      Get(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${MappingContextEndpoint.SEGMENT_CONTEXTS}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        val mappingContexts = JsonMethods.parse(responseAs[String]).extract[List[String]]
        mappingContexts.length shouldEqual 2
        mappingContexts.contains(mappingContext1) shouldEqual true
        mappingContexts.contains(mappingContext2) shouldEqual true
      }
    }

    "delete a mapping context within a project" in {
      Delete(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${MappingContextEndpoint.SEGMENT_CONTEXTS}/$mappingContext2") ~> route ~> check {
        status shouldEqual StatusCodes.NoContent
        // validate that mapping context is updated in projects.json file
        val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
        (projects.arr.find(p => (p \ "id").extract[String] == projectId).get \ "mappingContexts").asInstanceOf[JArray].arr.length shouldEqual 1
        // check mapping context file is deleted
        FileUtils.getPath(toFhirEngineConfig.mappingContextRepositoryFolderPath, projectId, mappingContext2).toFile shouldNot exist
      }
    }

    "upload a csv content to a mapping context" in {
      // get file from resources
      val file: File = FileOperations.getFileIfExists(getClass.getResource("/test-mapping-context/sample-unit-conversion.csv").getPath)
      val fileData = Multipart.FormData.BodyPart.fromPath("attachment", ContentTypes.`text/plain(UTF-8)`, file.toPath)
      val formData = Multipart.FormData(fileData)
      // save a csv file to mapping context
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${MappingContextEndpoint.SEGMENT_CONTEXTS}/$mappingContext1/${MappingContextEndpoint.SEGMENT_FILE}", formData.toEntity()) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[String] shouldEqual "OK"
        Thread.sleep(5000)
      }
    }

    "download csv content of a mapping context" in {
      // download csv content of the mapping context
      Get(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${MappingContextEndpoint.SEGMENT_CONTEXTS}/$mappingContext1/${MappingContextEndpoint.SEGMENT_FILE}") ~> route ~> check {
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

  /**
   * Creates a project to be used in the tests
   * */
  override def beforeAll(): Unit = {
    super.beforeAll()
    this.createProject()
  }

}
