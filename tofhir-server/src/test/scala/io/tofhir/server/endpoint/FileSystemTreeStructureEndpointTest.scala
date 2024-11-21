package io.tofhir.server.endpoint

import akka.http.scaladsl.model.StatusCodes
import io.tofhir.server.BaseEndpointTest
import io.tofhir.server.endpoint.FileSystemTreeStructureEndpoint
import io.tofhir.server.model.FilePathNode
import io.onfhir.definitions.common.model.Json4sSupport.formats
import io.tofhir.server.util.FileOperations
import org.json4s.jackson.JsonMethods

class FileSystemTreeStructureEndpointTest extends BaseEndpointTest {

  "File system tree structure endpoint" should {

    "retrieve all folders (only directories) under the given base path" in {
      Get(s"/${webServerConfig.baseUri}/${FileSystemTreeStructureEndpoint.SEGMENT_FILE_SYSTEM_PATH}?${FileSystemTreeStructureEndpoint.QUERY_PARAM_BASE_PATH}=.&${FileSystemTreeStructureEndpoint.QUERY_PARAM_INCLUDE_FILES}=false") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        val fileNode = JsonMethods.parse(responseAs[String]).extract[FilePathNode]
        fileNode.isFolder shouldEqual true
        fileNode.children.length shouldBe 8 // Only the folders since we called with include_files=false
        fileNode.children.head.isFolder shouldBe true
        fileNode.children.head.children shouldBe empty
      }
    }

    "retrieve all folders including all files under the given base path" in {
      Get(s"/${webServerConfig.baseUri}/${FileSystemTreeStructureEndpoint.SEGMENT_FILE_SYSTEM_PATH}?${FileSystemTreeStructureEndpoint.QUERY_PARAM_BASE_PATH}=.&${FileSystemTreeStructureEndpoint.QUERY_PARAM_INCLUDE_FILES}=true") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        val fileNode = JsonMethods.parse(responseAs[String]).extract[FilePathNode]
        fileNode.isFolder shouldEqual true
        fileNode.children.length shouldBe 9
        fileNode.children.count(_.label == "projects.json") shouldBe 1
        fileNode.children.count(_.isFolder) shouldBe 8
      }
    }

  }

}
