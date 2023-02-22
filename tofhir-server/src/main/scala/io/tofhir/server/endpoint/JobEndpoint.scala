package io.tofhir.server.endpoint

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.config.ToFhirEngineConfig
import io.tofhir.engine.model.FhirMappingJob
import io.tofhir.server.endpoint.JobEndpoint.SEGMENT_JOB
import io.tofhir.server.model.Json4sSupport._
import io.tofhir.server.model.ToFhirRestCall
import io.tofhir.server.service.JobService
import io.tofhir.server.service.project.IProjectRepository
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.util.FhirMappingJobFormatter.formats

class JobEndpoint(toFhirEngineConfig: ToFhirEngineConfig, projectRepository: IProjectRepository) extends LazyLogging {

  val service: JobService = new JobService(toFhirEngineConfig.jobRepositoryFolderPath, projectRepository)

  def route(request: ToFhirRestCall): Route = {
    pathPrefix(SEGMENT_JOB) {
      val projectId: String = request.projectId.get
      pathEndOrSingleSlash { // Operations on all mapping jobs
        getAllJobs(projectId) ~ createJob(projectId)
      } ~ pathPrefix(Segment) { id: String =>
          getJob(projectId, id) ~ updateJob(projectId, id) ~ deleteJob(projectId, id)
        }
    }
  }

  private def getAllJobs(projectId: String): Route = {
    get {
      complete {
        service.getAllMetadata(projectId)
      }
    }
  }

  private def createJob(projectId: String): Route = {
    post {
      entity(as[FhirMappingJob]) { job =>
        complete {
          service.createJob(projectId, job) map { created =>
            StatusCodes.Created -> created
          }
        }
      }
    }
  }

  private def getJob(projectId: String, id: String): Route = {
    get {
      complete {
        service.getJob(projectId, id) map {
          case Some(mappingJob) => StatusCodes.OK -> mappingJob
          case None => StatusCodes.NotFound -> s"Mapping with name $id not found"
        }
      }
    }
  }

  private def updateJob(projectId: String, id: String): Route = {
    put {
      entity(as[FhirMappingJob]) { job =>
        complete {
          service.updateJob(projectId, id, job) map { _ =>
            StatusCodes.OK -> job
          }
        }
      }
    }
  }

  private def deleteJob(projectId: String, id: String): Route = {
    delete {
      complete {
        service.deleteJob(projectId, id) map { _ =>
          StatusCodes.NoContent
        }
      }
    }
  }

}

object JobEndpoint {
  val SEGMENT_JOB = "jobs"
}
