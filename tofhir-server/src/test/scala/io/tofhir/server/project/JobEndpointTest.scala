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
  val job1: FhirMappingJob = FhirMappingJob(name = Some("mappingJob1"), sourceSettings = Map.empty, sinkSettings = sinkSettings, mappings = Seq.empty, mappingErrorHandling = ErrorHandlingType.CONTINUE)

  //  FileUtils.getPath(toFhirEngineConfig.mappingRepositoryFolderPath)
  val dataSourceSettings: Map[String, DataSourceSettings] =
    Map("source" ->
      FileSystemSourceSettings("test-source", "https://aiccelerate.eu/data-integration-suite/test-data", Paths.get(getClass.getResource("/test-data").toURI).normalize().toAbsolutePath.toString))

  val mappingFile: File = FileOperations.getFileIfExists(getClass.getResource("/patient-mapping.json").getPath)
  val mapping: FhirMapping = FileOperations.readJsonContentAsObject[FhirMapping](mappingFile)

  val patientMappingTask: FhirMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/patient-mapping",
    sourceContext = Map("source" -> FileSystemSource(path = "patients.csv")),
    mapping = Some(mapping)
  )
  val resourceFilter: ResourceFilter = ResourceFilter(numberOfRows = 3, order = "start")
  val patientMappingTaskWithFilter: TestResourceCreationRequest = TestResourceCreationRequest(
    fhirMappingTask = patientMappingTask,
    resourceFilter = resourceFilter
  )

  val patientMappingSchemaFile: File = FileOperations.getFileIfExists(getClass.getResource("/patient-schema.json").getPath)
  val patientMappingSchema: SchemaDefinition = FileOperations.readJsonContentAsObject[SchemaDefinition](patientMappingSchemaFile)

  // second job to be created
  val job2: FhirMappingJob = FhirMappingJob(name = Some("mappingJob2"), sourceSettings = dataSourceSettings, sinkSettings = sinkSettings, mappings = Seq(patientMappingTask), mappingErrorHandling = ErrorHandlingType.CONTINUE)

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

    "execute a mapping within a job and project and check mapped resources" in {
      // create the mapping that will be tested
      Post(s"/${webServerConfig.baseUri}/projects/${projectId}/mappings", HttpEntity(ContentTypes.`application/json`, writePretty(mapping))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that mapping metadata file is updated
        val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
        (projects.arr.find(p => (p \ "id").extract[String] == projectId).get \ "mappings").asInstanceOf[JArray].arr.length shouldEqual 1
        // check mapping folder is created
        FileUtils.getPath(toFhirEngineConfig.mappingRepositoryFolderPath, projectId, mapping.id).toFile.exists()
      }
      // create the schema that the mapping uses
      Post(s"/tofhir/projects/${projectId}/schemas", HttpEntity(ContentTypes.`application/json`, writePretty(patientMappingSchema))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that schema metadata file is updated
        val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
        (projects.arr.find(p => (p \ "id").extract[String] == projectId).get \ "schemas").asInstanceOf[JArray].arr.length shouldEqual 1
        // check schema folder is created
        FileUtils.getPath(toFhirEngineConfig.schemaRepositoryFolderPath, projectId, patientMappingSchema.id).toFile.exists()
      }
      // test a mapping
      Post(s"/${webServerConfig.baseUri}/projects/${projectId}/jobs/${job2.id}/test", HttpEntity(ContentTypes.`application/json`, writePretty(patientMappingTaskWithFilter))) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        val results: Seq[FhirMappingResult] = JsonMethods.parse(responseAs[String]).extract[Seq[FhirMappingResult]]
        results.length shouldEqual 3
        results.head.mappingUrl shouldEqual "https://aiccelerate.eu/fhir/mappings/pilot1/patient-mapping"
        results.head.mappedResource.get shouldEqual "{\"resourceType\":\"Patient\"," +
          "\"id\":\"34dc88d5972fd5472a942fc80f69f35c\"," +
          "\"meta\":{\"profile\":[\"https://aiccelerate.eu/fhir/StructureDefinition/AIC-Patient\"]," +
          "\"source\":\"https://aiccelerate.eu/data-integration-suite/test-data\"}," +
          "\"active\":true,\"identifier\":[{\"use\":\"official\",\"system\":\"https://aiccelerate.eu/data-integration-suite/test-data\",\"value\":\"p1\"}]," +
          "\"gender\":\"male\",\"birthDate\":\"2000-05-10 00:00:00\"}"
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
