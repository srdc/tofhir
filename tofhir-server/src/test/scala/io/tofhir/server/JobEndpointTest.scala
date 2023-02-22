package io.tofhir.server

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import io.tofhir.engine.config.{ErrorHandlingType, ToFhirEngineConfig}
import io.tofhir.engine.model.{FhirMappingJob, FhirSinkSettings, FileSystemSinkSettings}
import io.tofhir.engine.util.FhirMappingJobFormatter.formats
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.config.WebServerConfig
import io.tofhir.server.endpoint.ToFhirServerEndpoint
import io.tofhir.server.fhir.FhirDefinitionsConfig
import io.tofhir.server.model.{JobMetadata, Project}
import io.tofhir.server.service.project.ProjectFolderRepository
import io.tofhir.server.util.FileOperations
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.Serialization.writePretty
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.File

class JobEndpointTest extends AnyWordSpec with Matchers with ScalatestRouteTest with BeforeAndAfterAll {
  // toFHIR engine config
  val toFhirEngineConfig: ToFhirEngineConfig = new ToFhirEngineConfig(system.settings.config.getConfig("tofhir"))

  val webServerConfig = new WebServerConfig(system.settings.config.getConfig("webserver"))
  val fhirDefinitionsConfig = new FhirDefinitionsConfig(system.settings.config.getConfig("fhir"))
  var endpoint: ToFhirServerEndpoint = _
  // route endpoint
  var route: Route = _
  // first project to be created
  val project1: Project = Project(name = "example", description = Some("example project"))
  // first job to be created
  val sinkSettings: FhirSinkSettings = FileSystemSinkSettings(path = "http://example.com/fhir")
  val job1: FhirMappingJob = FhirMappingJob(name = Some("mappingJob1"), sourceSettings = Map.empty, sinkSettings = sinkSettings, mappings = Seq.empty, mappingErrorHandling = ErrorHandlingType.CONTINUE)
  // second job to be created
  val job2: FhirMappingJob = FhirMappingJob(name = Some("mappingJob2"), sourceSettings = Map.empty, sinkSettings = sinkSettings, mappings = Seq.empty, mappingErrorHandling = ErrorHandlingType.CONTINUE)
  // project created
  var createdProject1: Project = _

  "The service" should {

    "create a job within project" in {
      // create the first job
      Post(s"/${webServerConfig.baseUri}/projects/${createdProject1.id}/jobs", HttpEntity(ContentTypes.`application/json`, writePretty(job1))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that job metadata file is updated
        val projects = FileOperations.readJsonContent[Project](FileUtils.getPath(toFhirEngineConfig.toFhirDbFolderPath, ProjectFolderRepository.PROJECTS_JSON).toFile)
        projects.find(_.id == createdProject1.id).get.mappingJobs.length shouldEqual 1
        // check job folder is created
        FileUtils.getPath(toFhirEngineConfig.jobRepositoryFolderPath, createdProject1.id, job1.id).toFile.exists()
      }
      // create the second job
      Post(s"/${webServerConfig.baseUri}/projects/${createdProject1.id}/jobs", HttpEntity(ContentTypes.`application/json`, writePretty(job2))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that job metadata file is updated
        val projects = FileOperations.readJsonContent[Project](FileUtils.getPath(toFhirEngineConfig.toFhirDbFolderPath, ProjectFolderRepository.PROJECTS_JSON).toFile)
        projects.find(_.id == createdProject1.id).get.mappingJobs.length shouldEqual 2
        FileUtils.getPath(toFhirEngineConfig.jobRepositoryFolderPath, createdProject1.id, job2.id).toFile.exists()
      }
    }

    "get all jobs in a project" in {
      // get all jobs
      Get(s"/${webServerConfig.baseUri}/projects/${createdProject1.id}/jobs") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate the retrieved jobs
        val jobs: Seq[JobMetadata] = JsonMethods.parse(responseAs[String]).extract[Seq[JobMetadata]]
        jobs.length shouldEqual 2
      }
    }

    "get a job in a project" in {
      // get a job
      Get(s"/${webServerConfig.baseUri}/projects/${createdProject1.id}/jobs/${job1.id}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate the retrieved job
        val job: FhirMappingJob = JsonMethods.parse(responseAs[String]).extract[FhirMappingJob]
        job.name shouldEqual job1.name
      }
      // get a job with invalid id
      Get(s"/${webServerConfig.baseUri}/projects/${createdProject1.id}/jobs/123123") ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "update a job in a project" in {
      // update a job
      Put(s"/${webServerConfig.baseUri}/projects/${createdProject1.id}/jobs/${job1.id}", HttpEntity(ContentTypes.`application/json`, writePretty(job1.copy(name = Some("updatedJob"))))) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate the updated job
        val job: FhirMappingJob = JsonMethods.parse(responseAs[String]).extract[FhirMappingJob]
        job.name shouldEqual Some("updatedJob")
      }
      // update a job with invalid id
      Put(s"/${webServerConfig.baseUri}/projects/${createdProject1.id}/jobs/123123", HttpEntity(ContentTypes.`application/json`, writePretty(job1.copy(id = "123123")))) ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "delete a job in a project" in {
      // delete a job
      Delete(s"/${webServerConfig.baseUri}/projects/${createdProject1.id}/jobs/${job1.id}") ~> route ~> check {
        status shouldEqual StatusCodes.NoContent
        // validate that job metadata file is updated
        val projects = FileOperations.readJsonContent[Project](FileUtils.getPath(toFhirEngineConfig.toFhirDbFolderPath, ProjectFolderRepository.PROJECTS_JSON).toFile)
        projects.find(_.id == createdProject1.id).get.mappingJobs.length shouldEqual 1
        // check job folder is deleted
        FileUtils.getPath(toFhirEngineConfig.jobRepositoryFolderPath, createdProject1.id, job1.id).toFile.exists() shouldEqual false
      }
      // delete a job with invalid id
      Delete(s"/${webServerConfig.baseUri}/projects/${createdProject1.id}/jobs/123123") ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }
  }

  /**
   * Creates a repository folder before tests are run.
   * */
  override def beforeAll(): Unit = {
    new File(toFhirEngineConfig.toFhirDbFolderPath).mkdir()
    // initialize endpoint and route
    endpoint = new ToFhirServerEndpoint(toFhirEngineConfig, webServerConfig, fhirDefinitionsConfig)
    route = endpoint.toFHIRRoute
    // create a project
    Post("/tofhir/projects", HttpEntity(ContentTypes.`application/json`, writePretty(project1))) ~> route ~> check {
      status shouldEqual StatusCodes.Created
      val project: Project = JsonMethods.parse(responseAs[String]).extract[Project]
      // set the created project
      createdProject1 = project
    }
  }

  /**
   * Deletes the repository folder after all test cases are completed.
   * */
  override def afterAll(): Unit = {
    org.apache.commons.io.FileUtils.deleteDirectory(new File(toFhirEngineConfig.toFhirDbFolderPath))
    org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(toFhirEngineConfig.jobRepositoryFolderPath, createdProject1.id).toFile)
  }

}
