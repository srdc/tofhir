package io.tofhir.server.endpoint

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.RouteTestTimeout
import akka.testkit.TestDuration
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.BaseEndpointTest
import io.tofhir.server.model.Project
import io.onfhir.definitions.common.model.Json4sSupport.formats
import io.tofhir.server.repository.project.ProjectFolderRepository
import io.tofhir.server.util.FileOperations
import org.json4s.{JArray, JObject, JString}
import org.json4s.jackson.{JsonMethods, Serialization}

import java.io.FileWriter
import scala.concurrent.duration.DurationInt

class ReloadingEndpointTest extends BaseEndpointTest {

  // default timeout is 1 seconds, which is not enough for this test
  implicit def default(implicit system: ActorSystem): RouteTestTimeout = RouteTestTimeout(new DurationInt(10).second.dilated(system))

  "The service" should {

    "should reload projects successfully after updating project file" in {
      // Read projects and update description fields from projects.json
      val projectsFile = FileUtils.getPath(ProjectFolderRepository.PROJECTS_JSON).toFile
      val parsedProjects = FileOperations.readFileIntoJson(projectsFile).asInstanceOf[JArray].arr.map(p => p.asInstanceOf[JObject])
      val reloadedProjects = parsedProjects.map(project => {
        project.mapField {
          case ("description", _) => ("description", JString("reloaded"))
          case otherwise => otherwise
        }
      })
      val fw = new FileWriter(projectsFile)
      try fw.write(Serialization.writePretty(reloadedProjects)) finally fw.close()

      // Trigger reload endpoint
      Get(s"/${webServerConfig.baseUri}/${ReloadEndpoint.SEGMENT_RELOAD}") ~> route ~> check {
        status shouldEqual StatusCodes.NoContent

        // Check project endpoint for whether project descriptions are updated
        Get(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}") ~> route ~> check {
          status shouldEqual StatusCodes.OK
          val projects: Seq[Project] = JsonMethods.parse(responseAs[String]).extract[Seq[Project]]
          projects.foreach(project => {
            project.description.get shouldEqual "reloaded"
          })
        }
      }
    }
  }
}
