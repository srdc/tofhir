package io.tofhir.server.endpoint

import akka.http.scaladsl.model.StatusCodes
import io.onfhir.definitions.common.model.Json4sSupport.formats
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.BaseEndpointTest
import io.tofhir.server.model.Project
import io.tofhir.server.repository.project.ProjectFolderRepository
import io.tofhir.server.util.FileOperations
import org.json4s.jackson.JsonMethods
import org.json4s.{JArray, JObject}


class ReloadingEndpointTest extends BaseEndpointTest {

  "The service" should {

    "should reload projects successfully after updating project file" in {
      // Read number of projects
      val projectsFile = FileUtils.getPath(ProjectFolderRepository.PROJECTS_JSON).toFile
      val parsedProjects = FileOperations.readFileIntoJson(projectsFile).asInstanceOf[JArray].arr.map(p => p.asInstanceOf[JObject])
      val numberOfprojects = parsedProjects.length
      // Delete projects.json
      projectsFile.delete()

      // Trigger reload endpoint
      Get(s"/${webServerConfig.baseUri}/${ReloadEndpoint.SEGMENT_RELOAD}") ~> route ~> check {
        status shouldEqual StatusCodes.NoContent

        // Check project endpoint for whether number of created projects is correct
        Get(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}") ~> route ~> check {
          status shouldEqual StatusCodes.OK
          val projects: Seq[Project] = JsonMethods.parse(responseAs[String]).extract[Seq[Project]]
          projects.length shouldEqual numberOfprojects
        }
      }
    }
  }
}
