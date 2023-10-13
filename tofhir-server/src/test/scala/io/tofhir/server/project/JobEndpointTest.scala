package io.tofhir.server.project

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.testkit.RouteTestTimeout
import akka.testkit.TestDuration
import io.tofhir.common.model.SchemaDefinition
import io.tofhir.engine.config.ErrorHandlingType
import io.tofhir.engine.model._
import io.tofhir.engine.util.FhirMappingJobFormatter.formats
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.BaseEndpointTest
import io.tofhir.server.model.{ResourceFilter, TestResourceCreationRequest}
import io.tofhir.server.util.{FileOperations, TestUtil}
import org.json4s.JArray
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.Serialization.writePretty

import java.io.File
import java.nio.file.Paths
import scala.concurrent.duration.DurationInt

class JobEndpointTest extends BaseEndpointTest {
  // default timeout is 1 seconds, which is not enough for some tests
  implicit def default(implicit system: ActorSystem) = RouteTestTimeout(new DurationInt(60).second.dilated(system))

  // first job to be created
  val sinkSettings: FhirSinkSettings = FileSystemSinkSettings(path = "http://example.com/fhir")
  val job1: FhirMappingJob = FhirMappingJob(name = Some("mappingJob1"), sourceSettings = Map.empty, sinkSettings = sinkSettings, mappings = Seq.empty, dataProcessingSettings = DataProcessingSettings())

  "The service" should {

    "create a job within project" in {
      // create the first job
      Post(s"/${webServerConfig.baseUri}/projects/${projectId}/jobs", HttpEntity(ContentTypes.`application/json`, writePretty(job1))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that job metadata file is updated
        val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
        (projects.arr.find(p => (p \ "id").extract[String] == projectId).get \ "mappingJobs").asInstanceOf[JArray].arr.length shouldEqual 1
        // check job folder is created
        FileUtils.getPath(toFhirEngineConfig.jobRepositoryFolderPath, projectId, job1.id).toFile.exists()
      }
    }

    "get all jobs in a project" in {
      // get all jobs
      Get(s"/${webServerConfig.baseUri}/projects/${projectId}/jobs") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate the retrieved jobs
        val jobs: Seq[FhirMappingJob] = JsonMethods.parse(responseAs[String]).extract[Seq[FhirMappingJob]]
        jobs.length shouldEqual 1
      }
    }

    "get a job in a project" in {
      // get a job
      Get(s"/${webServerConfig.baseUri}/projects/${projectId}/jobs/${job1.id}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate the retrieved job
        val job: FhirMappingJob = JsonMethods.parse(responseAs[String]).extract[FhirMappingJob]
        job.name shouldEqual job1.name
      }
      // get a job with invalid id
      Get(s"/${webServerConfig.baseUri}/projects/${projectId}/jobs/123123") ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "update a job in a project" in {
      // update a job
      Put(s"/${webServerConfig.baseUri}/projects/${projectId}/jobs/${job1.id}", HttpEntity(ContentTypes.`application/json`, writePretty(job1.copy(name = Some("updatedJob"))))) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate the updated job
        val job: FhirMappingJob = JsonMethods.parse(responseAs[String]).extract[FhirMappingJob]
        job.name shouldEqual Some("updatedJob")
      }
      // update a job with invalid id
      Put(s"/${webServerConfig.baseUri}/projects/${projectId}/jobs/123123", HttpEntity(ContentTypes.`application/json`, writePretty(job1.copy(id = "123123")))) ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "delete a job in a project" in {
      // delete a job
      Delete(s"/${webServerConfig.baseUri}/projects/${projectId}/jobs/${job1.id}") ~> route ~> check {
        status shouldEqual StatusCodes.NoContent
        // validate that job metadata file is updated
        val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
        (projects.arr.find(p => (p \ "id").extract[String] == projectId).get \ "mappingJobs").asInstanceOf[JArray].arr.length shouldEqual 0
        // check job folder is deleted
        FileUtils.getPath(toFhirEngineConfig.jobRepositoryFolderPath, projectId, job1.id).toFile.exists() shouldEqual false
      }
      // delete a job with invalid id
      Delete(s"/${webServerConfig.baseUri}/projects/${projectId}/jobs/123123") ~> route ~> check {
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
