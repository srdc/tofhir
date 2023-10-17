package io.tofhir.server.endpoint

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.server.endpoint.ProjectEndpoint.SEGMENT_PROJECTS
import io.tofhir.server.model.ToFhirRestCall

/**
 * Endpoints to manage projects.
 * */
class ProjectEndpoint() extends LazyLogging {

  val jobEndpoint: JobEndpoint = new JobEndpoint()

  def route(request: ToFhirRestCall): Route = {
    pathPrefix(SEGMENT_PROJECTS) {
      pathPrefix(Segment) { projectId: String =>
          {
            request.projectId = Some(projectId)
            jobEndpoint.route(request)
          }
        }
    }
  }
}

object ProjectEndpoint {
  val SEGMENT_PROJECTS = "projects"
}



