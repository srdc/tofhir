package io.tofhir.server.endpoint

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.testkit.RouteTestTimeout
import akka.testkit.TestDuration
import io.tofhir.engine.data.write.FileSystemWriter.SinkContentTypes
import io.tofhir.engine.model._
import io.tofhir.engine.util.FhirMappingJobFormatter.formats
import io.tofhir.engine.util.FileUtils
import io.tofhir.engine.util.FileUtils.FileExtensions
import io.tofhir.server.BaseEndpointTest
import io.tofhir.server.util.TestUtil
import org.json4s.JArray
import org.json4s.JsonAST.{JBool, JString, JValue}
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.Serialization.writePretty

import scala.concurrent.duration.DurationInt

class JobEndpointTest extends BaseEndpointTest {
  // default timeout is 1 seconds, which is not enough for some tests
  implicit def default(implicit system: ActorSystem): RouteTestTimeout = RouteTestTimeout(new DurationInt(60).second.dilated(system))

  // first job to be created
  val sinkSettings: FhirSinkSettings = FileSystemSinkSettings(path = "http://example.com/fhir", contentType = SinkContentTypes.CSV)
  val job1: FhirMappingJob = FhirMappingJob(name = Some("mappingJob1"), sourceSettings = Map.empty, sinkSettings = sinkSettings, mappings = Seq.empty, dataProcessingSettings = DataProcessingSettings())
  // second job using kafka as a data source to be created
  val kafkaSourceSettings: KafkaSourceSettings = KafkaSourceSettings(name = "kafka-source", sourceUri = "http://example.com/kafka", bootstrapServers = "http://some-kafka-server:9092")
  val mappingJobSourceSettings: Map[String, MappingJobSourceSettings] =
    Map("source1" -> kafkaSourceSettings)
  val kafkaMappingTask: Seq[FhirMappingTask] = Seq(FhirMappingTask("mappingTest1", "mappingRef1", Map("sourceBinding1" -> KafkaSource(topicName = "topic", sourceRef = Some("source1"), options = Map("startingOffsets" -> "latest")))))
  val kafkaSourceJob: FhirMappingJob = FhirMappingJob(name = Some("mappingJob2"), sourceSettings = mappingJobSourceSettings, sinkSettings = sinkSettings, mappings = kafkaMappingTask, dataProcessingSettings = DataProcessingSettings())
  // a malformed job with a source reference to a missing data source in the mapping tasks, to be rejected
  val malformedMappingTasks: Seq[FhirMappingTask] = Seq(FhirMappingTask("mappingTest2","mappingRef1", Map("sourceBinding1" -> SqlSource(tableName = Some("table"), sourceRef = Some("source2")))))
  val mappingTaskMalformedJob: FhirMappingJob = FhirMappingJob(name = Some("malformedJob1"), sourceSettings = mappingJobSourceSettings, sinkSettings = sinkSettings, mappings = malformedMappingTasks, dataProcessingSettings = DataProcessingSettings())
  // a malformed job which is a scheduling job and has a stream file system data source, to be rejected
  val streamFileSystemSourceSettings: FileSystemSourceSettings = FileSystemSourceSettings(name = "file-system-source", sourceUri = "http://example.co/filesystem", dataFolderPath = "test/data", asStream = true)
  val streamAndSchedulingMalformedJob: FhirMappingJob = FhirMappingJob(name = Some("malformedJob2"), schedulingSettings = Some(SchedulingSettings(cronExpression = "* * * * *")) ,sourceSettings = Map("source1" -> streamFileSystemSourceSettings.copy(asStream = false), "source2" -> streamFileSystemSourceSettings), sinkSettings = sinkSettings, mappings = Seq.empty, dataProcessingSettings = DataProcessingSettings())

  "The service" should {

    "create a job within project" in {
      // create the first job
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${JobEndpoint.SEGMENT_JOB}", HttpEntity(ContentTypes.`application/json`, writePretty(job1))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that job metadata file is updated
        val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
        (projects.arr.find(p => (p \ "id").extract[String] == projectId).get \ "mappingJobs").asInstanceOf[JArray].arr.length shouldEqual 1
        // check job folder is created
        FileUtils.getPath(toFhirEngineConfig.jobRepositoryFolderPath, projectId, s"${job1.id}${FileExtensions.JSON}").toFile should exist
      }

      // create the second job
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${JobEndpoint.SEGMENT_JOB}", HttpEntity(ContentTypes.`application/json`, writePretty(kafkaSourceJob))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that job metadata file is updated
        val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
        (projects.arr.find(p => (p \ "id").extract[String] == projectId).get \ "mappingJobs").asInstanceOf[JArray].arr.length shouldEqual 2
        // check job file is created
        FileUtils.getPath(toFhirEngineConfig.jobRepositoryFolderPath, projectId, s"${kafkaSourceJob.id}${FileExtensions.JSON}").toFile should exist
      }
    }

    "get all jobs in a project" in {
      // get all jobs
      Get(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${JobEndpoint.SEGMENT_JOB}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate the retrieved jobs
        val jobs: Seq[FhirMappingJob] = JsonMethods.parse(responseAs[String]).extract[Seq[FhirMappingJob]]
        jobs.length shouldEqual 2
      }
    }

    "get a job in a project" in {
      // get a job
      Get(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${JobEndpoint.SEGMENT_JOB}/${job1.id}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate the retrieved job
        val job: FhirMappingJob = JsonMethods.parse(responseAs[String]).extract[FhirMappingJob]
        job.name shouldEqual job1.name
      }
      // get a job with invalid id
      Get(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${JobEndpoint.SEGMENT_JOB}/123123") ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "update a job in a project" in {
      // update a job
      Put(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${JobEndpoint.SEGMENT_JOB}/${job1.id}", HttpEntity(ContentTypes.`application/json`, writePretty(job1.copy(name = Some("updatedJob"))))) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate the updated job
        val job: FhirMappingJob = JsonMethods.parse(responseAs[String]).extract[FhirMappingJob]
        job.name shouldEqual Some("updatedJob")
      }
      // update a job with invalid id
      Put(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${JobEndpoint.SEGMENT_JOB}/123123", HttpEntity(ContentTypes.`application/json`, writePretty(job1.copy(id = "123123")))) ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "delete a job in a project" in {
      // delete a job
      Delete(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${JobEndpoint.SEGMENT_JOB}/${job1.id}") ~> route ~> check {
        status shouldEqual StatusCodes.NoContent
        // validate that job metadata file is updated
        val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
        (projects.arr.find(p => (p \ "id").extract[String] == projectId).get \ "mappingJobs").asInstanceOf[JArray].arr.length shouldEqual 1
        // check job folder is deleted
        FileUtils.getPath(toFhirEngineConfig.jobRepositoryFolderPath, projectId, s"${job1.id}${FileExtensions.JSON}").toFile shouldNot exist
      }
      // delete a job with invalid id
      Delete(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${JobEndpoint.SEGMENT_JOB}/123123") ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "get a job with kafka source type in a project" in {
      // get a job
      Get(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${JobEndpoint.SEGMENT_JOB}/${kafkaSourceJob.id}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // convert the JSON response to a JValue
        val jsonResponse: JValue = JsonMethods.parse(responseAs[String])
        // check if the fields exist
        (jsonResponse \ "name") should be (JString("mappingJob2"))
        (jsonResponse \ "sourceSettings" \ "source1" \ "asStream") should be (JBool(true))
      }
    }

    "prevent creation of jobs with malformed content" in {
      // try to create a job with malformed mapping content
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${JobEndpoint.SEGMENT_JOB}", HttpEntity(ContentTypes.`application/json`, writePretty(mappingTaskMalformedJob))) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        // validate error message
        val response = responseAs[String]
        response should include("Detail: The data source referenced by source name 'source2' in the mapping task 'mappingTest2' is not found in the source settings of the job.")
        // validate that job metadata file is still the same
        val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
        (projects.arr.find(p => (p \ "id").extract[String] == projectId).get \ "mappingJobs").asInstanceOf[JArray].arr.length shouldEqual 1
        // check job folder is not created
        FileUtils.getPath(toFhirEngineConfig.jobRepositoryFolderPath, projectId, s"${mappingTaskMalformedJob.id}${FileExtensions.JSON}").toFile shouldNot exist
      }

      // try to crate a job which is scheduling and has a stream data source
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${JobEndpoint.SEGMENT_JOB}", HttpEntity(ContentTypes.`application/json`, writePretty(streamAndSchedulingMalformedJob))) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        // validate error message
        val response = responseAs[String]
        response should include("Detail: Streaming jobs cannot be scheduled.")
        // validate that job metadata file is still the same
        val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
        (projects.arr.find(p => (p \ "id").extract[String] == projectId).get \ "mappingJobs").asInstanceOf[JArray].arr.length shouldEqual 1
        // check job folder is not created
        FileUtils.getPath(toFhirEngineConfig.jobRepositoryFolderPath, projectId, s"${mappingTaskMalformedJob.id}${FileExtensions.JSON}").toFile shouldNot exist
      }
    }
  }


  "prevent creation of a job with two mappingTask share same name" in {

    val mappingTask: FhirMappingTask = FhirMappingTask("testname1", "mappingref1", sourceBinding = Map.empty)
    val mappingTask2: FhirMappingTask = FhirMappingTask("testname1", "mappingref1", sourceBinding = Map.empty)
    val jobWithSameMappingTaskName = job1.copy(id = "jobWithSameMappingTaskName", name = Option("jobWithSameMappingTaskName"),mappings = Seq(mappingTask, mappingTask2))
    // try to create a job with two mappingTask share same name
    Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${JobEndpoint.SEGMENT_JOB}", HttpEntity(ContentTypes.`application/json`, writePretty(jobWithSameMappingTaskName))) ~> route ~> check {
      status shouldEqual StatusCodes.BadRequest
      // validate error message
      val response = responseAs[String]
      response should include("Detail: Duplicate MappingTask name(s) found: testname1. Each MappingTask must have a unique name.")
      // validate that job metadata file is still the same
      val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
      (projects.arr.find(p => (p \ "id").extract[String] == projectId).get \ "mappingJobs").asInstanceOf[JArray].arr.length shouldEqual 1
      // check job folder is not created
      FileUtils.getPath(toFhirEngineConfig.jobRepositoryFolderPath, projectId, s"${jobWithSameMappingTaskName.id}${FileExtensions.JSON}").toFile shouldNot exist
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
