package io.tofhir.server.endpoint

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.model.FhirMappingJob
import io.tofhir.server.endpoint.JobEndpoint.{SEGMENT_JOB, SEGMENT_MONITORING, SEGMENT_RUN}
import io.tofhir.server.model.Json4sSupport._
import io.tofhir.server.model.ToFhirRestCall
import io.tofhir.server.service.JobService
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.util.FhirMappingJobFormatter.formats
import io.tofhir.server.service.job.IJobRepository

class JobEndpoint(jobRepository: IJobRepository) extends LazyLogging {

  val service: JobService = new JobService(jobRepository)

  def route(request: ToFhirRestCall): Route = {
    pathPrefix(SEGMENT_JOB) {
      val projectId: String = request.projectId.get
      pathEndOrSingleSlash { // Operations on all mapping jobs
        getAllJobs(projectId) ~ createJob(projectId)
      } ~ pathPrefix(Segment) { id: String =>
        pathEndOrSingleSlash { // Operations on all mapping jobs
          getJob(projectId, id) ~ updateJob(projectId, id) ~ deleteJob(projectId, id)
        } ~ pathPrefix(SEGMENT_RUN) { // run a mapping job
          pathEndOrSingleSlash {
            runJob(projectId, id)
          }
        } ~ pathPrefix(SEGMENT_MONITORING) { // monitor the result of a mapping job
          pathEndOrSingleSlash {
            monitorJob(projectId, id)
          }
        }
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

  private def runJob(projectId: String, id: String): Route = {
    post {
      complete {
        service.runJob(projectId, id) map { _ =>
          StatusCodes.OK
        }
      }
    }
  }

  /**
   * Route to monitor the result of a mapping job
   * */
  private def monitorJob(projectId: String, id: String): Route = {
    get {
      parameterMap { queryParams => // page is supported for now (e.g. page=1)
        complete {
          service.monitorJob(projectId, id, queryParams)
        }
      }
    }
  }
}

object JobEndpoint {
  val SEGMENT_JOB = "jobs"
  val SEGMENT_RUN = "run"
  val SEGMENT_MONITORING = "monitoring"
}
