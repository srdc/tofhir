package io.tofhir.server.endpoint

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.model.{FhirMappingJob, FhirMappingTask}
import io.tofhir.server.endpoint.JobEndpoint.{SEGMENT_EXECUTIONS, SEGMENT_JOB, SEGMENT_RUN, SEGMENT_TEST}
import io.tofhir.server.model.Json4sSupport._
import io.tofhir.server.model.{FhirMappingTaskTest, RowSelectionOrder, ToFhirRestCall}
import io.tofhir.server.service.JobService
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.util.FhirMappingJobFormatter.formats
import io.tofhir.server.interceptor.ICORSHandler
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
        } ~ pathPrefix(SEGMENT_TEST) { // test a mapping with mapping job configurations
          pathEndOrSingleSlash {
            testMappingWithJob(projectId, id)
          }
        } ~ pathPrefix(SEGMENT_EXECUTIONS) { // Operations on all executions
          pathEndOrSingleSlash {
            getExecutions(projectId, id)
          } ~ pathPrefix(Segment) { executionId: String => // operations on an execution
            getExecutionLogs(executionId)
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
      entity(as[Option[Seq[String]]]) { mappingUrls =>
        complete {
          service.runJob(projectId, id,mappingUrls) map { _ =>
            StatusCodes.OK
          }
        }
      }
    }
  }

  /**
   * Route to test a mapping with mapping job configurations i.e. source data configurations
   * */
  private def testMappingWithJob(projectId: String, id: String): Route = {
    post {
      entity(as[FhirMappingTaskTest]) { mappingTaskTest =>
        validate(RowSelectionOrder.isValid(mappingTaskTest.selection.order),
          "Invalid row selection order. Available options are: start, end, random") {
          complete {
            service.testMappingWithJob(projectId, id, mappingTaskTest)
          }
        }
      }
    }
  }

  /**
   * Route to get executions of a mapping job
   * */
  private def getExecutions(projectId: String, id: String): Route = {
    get {
      parameterMap { queryParams => // page is supported for now (e.g. page=1)
        onComplete(service.getExecutions(projectId, id, queryParams)){
          case util.Success(response) =>
            val headers = List(
              RawHeader(ICORSHandler.X_TOTAL_COUNT_HEADER, response._2.toString)
            )
            respondWithHeaders(headers){
              complete(response._1)
            }
        }
      }
    }
  }

  /**
   * Route to retrieve execution logs i.e. the logs of mapping task which are ran in the execution
   * */
  private def getExecutionLogs(id: String): Route = {
    get {
      complete {
        service.getExecutionLogs(id)
      }
    }
  }
}

object JobEndpoint {
  val SEGMENT_JOB = "jobs"
  val SEGMENT_RUN = "run"
  val SEGMENT_EXECUTIONS = "executions"
  val SEGMENT_TEST = "test"
}
