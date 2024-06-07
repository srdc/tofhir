package io.tofhir.server.project

import akka.http.scaladsl.model.StatusCodes
import io.tofhir.server.BaseEndpointTest
import io.tofhir.server.endpoint.FileSystemTreeStructureEndpoint
import io.tofhir.server.model.FilePathNode
import io.tofhir.common.model.Json4sSupport.formats
import io.tofhir.server.util.FileOperations
import org.json4s.jackson.JsonMethods

class FileSystemTreeStructureEndpointTest extends BaseEndpointTest {

  "File system tree structure endpoint" should {

    "retrieve all folders (only directories) under the given base path" in {
      val fileSystemStructureTestFolder = FileOperations.getFileIfExists(getClass.getResource("/file-system-structure").getPath)
      Get(s"/${webServerConfig.baseUri}/${FileSystemTreeStructureEndpoint.SEGMENT_FILE_SYSTEM_PATH}?${FileSystemTreeStructureEndpoint.QUERY_PARAM_BASE_PATH}=${fileSystemStructureTestFolder.getPath}&${FileSystemTreeStructureEndpoint.QUERY_PARAM_INCLUDE_FILES}=false") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        val fileNode = JsonMethods.parse(responseAs[String]).extract[FilePathNode]
        fileNode.label shouldEqual "file-system-structure"
        fileNode.children.length shouldBe 1 // Only the "subdir" folder since we called with include_files=false
        fileNode.children.head.isFolder shouldBe true
        fileNode.children.head.children shouldBe empty
      }
    }

    "retrieve all folders including all files under the given base path" in {
      val fileSystemStructureTestFolder = FileOperations.getFileIfExists(getClass.getResource("/file-system-structure").getPath)
      Get(s"/${webServerConfig.baseUri}/${FileSystemTreeStructureEndpoint.SEGMENT_FILE_SYSTEM_PATH}?${FileSystemTreeStructureEndpoint.QUERY_PARAM_BASE_PATH}=${fileSystemStructureTestFolder.getPath}&${FileSystemTreeStructureEndpoint.QUERY_PARAM_INCLUDE_FILES}=true") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        val fileNode = JsonMethods.parse(responseAs[String]).extract[FilePathNode]
        fileNode.label shouldEqual "file-system-structure"
        fileNode.children.length shouldBe 3
        fileNode.children.count(_.label == "file2.txt") shouldBe 1
        fileNode.children.count(_.isFolder) shouldBe 1
        fileNode.children.filter(_.isFolder).head.children.length shouldBe 1
      }
    }

  }

}
