package io.tofhir.server.project

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import io.tofhir.engine.config.ErrorHandlingType
import io.tofhir.engine.model.{FhirMappingJob, FhirSinkSettings, FileSystemSinkSettings}
import io.tofhir.engine.util.FhirMappingJobFormatter.formats
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.BaseEndpointTest
import io.tofhir.server.util.TestUtil
import org.json4s.JArray
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.Serialization.writePretty

class JobEndpointTest extends BaseEndpointTest {
  // first job to be created
  val sinkSettings: FhirSinkSettings = FileSystemSinkSettings(path = "http://example.com/fhir")
  val job1: FhirMappingJob = FhirMappingJob(name = Some("mappingJob1"), sourceSettings = Map.empty, sinkSettings = sinkSettings, mappings = Seq.empty, mappingErrorHandling = ErrorHandlingType.CONTINUE)
  // second job to be created
  val job2: FhirMappingJob = FhirMappingJob(name = Some("mappingJob2"), sourceSettings = Map.empty, sinkSettings = sinkSettings, mappings = Seq.empty, mappingErrorHandling = ErrorHandlingType.CONTINUE)

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
      // create the second job
      Post(s"/${webServerConfig.baseUri}/projects/${projectId}/jobs", HttpEntity(ContentTypes.`application/json`, writePretty(job2))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that job metadata file is updated
        val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
        (projects.arr.find(p => (p \ "id").extract[String] == projectId).get \ "mappingJobs").asInstanceOf[JArray].arr.length shouldEqual 2
        FileUtils.getPath(toFhirEngineConfig.jobRepositoryFolderPath, projectId, job2.id).toFile.exists()
      }
    }

    "get all jobs in a project" in {
      // get all jobs
      Get(s"/${webServerConfig.baseUri}/projects/${projectId}/jobs") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate the retrieved jobs
        val jobs: Seq[FhirMappingJob] = JsonMethods.parse(responseAs[String]).extract[Seq[FhirMappingJob]]
        jobs.length shouldEqual 2
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
        (projects.arr.find(p => (p \ "id").extract[String] == projectId).get \ "mappingJobs").asInstanceOf[JArray].arr.length shouldEqual 1
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
