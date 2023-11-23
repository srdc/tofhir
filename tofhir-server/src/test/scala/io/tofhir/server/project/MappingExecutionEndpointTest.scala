package io.tofhir.server.project

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, Multipart, StatusCodes}
import akka.http.scaladsl.testkit.RouteTestTimeout
import akka.testkit.TestDuration
import io.tofhir.common.model.SchemaDefinition
import io.tofhir.engine.config.{ErrorHandlingType, ToFhirConfig}
import io.tofhir.engine.data.write.FileSystemWriter.SinkFileFormats
import io.tofhir.engine.model._
import io.tofhir.engine.util.FhirMappingJobFormatter.formats
import io.tofhir.engine.util.FileUtils
import io.tofhir.engine.util.FileUtils.FileExtensions
import io.tofhir.server.BaseEndpointTest
import io.tofhir.server.model.{ResourceFilter, TestResourceCreationRequest}
import io.tofhir.server.util.{FileOperations, TestUtil}
import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.Serialization.writePretty

import java.io.File
import java.nio.file.Paths
import java.util.UUID
import scala.concurrent.duration.DurationInt
import scala.util.control.Breaks.{break, breakable}

class MappingExecutionEndpointTest extends BaseEndpointTest {
  // default timeout is 1 seconds, which is not enough for some tests
  implicit def default(implicit system: ActorSystem) = RouteTestTimeout(new DurationInt(60).second.dilated(system))

  // first job to be created
  val fsSinkFolderName: String = "fsSink"
  val fsSinkFolder: File = new File(fsSinkFolderName)
  val sparkSession: SparkSession = SparkSession.builder().config(ToFhirConfig.createSparkConf).getOrCreate()

  val resourceFilter: ResourceFilter = ResourceFilter(numberOfRows = 3, order = "start")

  val job1Id: String = UUID.randomUUID().toString
  val job2Id: String = UUID.randomUUID().toString

  val dataSourceSettings: Map[String, DataSourceSettings] =
    Map("source" ->
      FileSystemSourceSettings("test-source", "https://aiccelerate.eu/data-integration-suite/test-data", Paths.get(getClass.getResource("/test-data").toURI).normalize().toAbsolutePath.toString))
  var sinkSettings: FhirSinkSettings = FileSystemSinkSettings(path = s"./$fsSinkFolderName/job1_1", Some(SinkFileFormats.NDJSON))
  val patientMappingTask: FhirMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot1/patient-mapping",
    sourceContext = Map("source" -> FileSystemSource(path = "patients.csv")),
    mapping = Some(FileOperations.readJsonContentAsObject[FhirMapping](FileOperations.getFileIfExists(getClass.getResource("/patient-mapping.json").getPath)))
  )
  val batchJob: FhirMappingJob = FhirMappingJob(
    id = job1Id,
    name = Some("mappingJob"),
    sourceSettings = dataSourceSettings,
    sinkSettings = sinkSettings,
    mappings = Seq(patientMappingTask),
    dataProcessingSettings = DataProcessingSettings(mappingErrorHandling = ErrorHandlingType.CONTINUE, saveErroneousRecords = true, archiveMode = ArchiveModes.OFF))

  // streaming job initialization
  val parentStreamingFolderName = "streaming-parent-folder"
  val parentStreamingFolder: File = new File(parentStreamingFolderName)

  val patientStreamingFolderName = "patients"
  val patientStreamingFolder: File = new File(patientStreamingFolderName)

  var sinkSettingsForStreaming: FhirSinkSettings = FileSystemSinkSettings(path = s"./$fsSinkFolderName/job3", Some(SinkFileFormats.NDJSON))
  val streamingDataSourceSettings: Map[String, DataSourceSettings] =
    Map("source" ->
      FileSystemSourceSettings("streaming-test-source", "https://some-url-for-data-source", Paths.get(getClass.getResource(s"/$parentStreamingFolderName").toURI).normalize().toAbsolutePath.toString, asStream = true))
  val patientStreamingMappingTask: FhirMappingTask = FhirMappingTask(
    mappingRef = "https://some-url-for-streaming-patient-mapping",
    sourceContext = Map("source" -> FileSystemSource(path = s"$patientStreamingFolderName", fileFormat = Some(SourceFileFormats.CSV))),
    mapping = Some(FileOperations.readJsonContentAsObject[FhirMapping](FileOperations.getFileIfExists(getClass.getResource("/patient-mapping.json").getPath)))
  )

  val streamingJob: FhirMappingJob = FhirMappingJob(
    id = UUID.randomUUID().toString,
    name = Some("streamingMappingJob"),
    sourceSettings = streamingDataSourceSettings,
    sinkSettings = sinkSettingsForStreaming,
    mappings = Seq(patientStreamingMappingTask),
    dataProcessingSettings = DataProcessingSettings(mappingErrorHandling = ErrorHandlingType.CONTINUE, saveErroneousRecords = true, archiveMode = ArchiveModes.OFF)
  )

  "The service" should {
    "run a job including a mapping" in {
      // Create the job
      Post(s"/${webServerConfig.baseUri}/projects/${projectId}/jobs", HttpEntity(ContentTypes.`application/json`, writePretty(batchJob))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that job metadata file is updated
        val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
        (projects.arr.find(p => (p \ "id").extract[String] == projectId).get \ "mappingJobs").asInstanceOf[JArray].arr.length shouldEqual 1
        // check job folder is created
        FileUtils.getPath(toFhirEngineConfig.jobRepositoryFolderPath, projectId, s"${batchJob.id}${FileExtensions.JSON}").toFile should exist
      }

      // Run the job
      Post(s"/${webServerConfig.baseUri}/projects/${projectId}/jobs/${batchJob.id}/run", HttpEntity(ContentTypes.`application/json`, "")) ~> route ~> check {
        status shouldEqual StatusCodes.OK

        // Mappings run asynchronously. Wait at most 10 seconds for mappings to complete.
        val success = waitForCondition(10) {
          fsSinkFolder.listFiles.exists(_.getName.contains("job1_1")) && {
            // Check the resources created in the file system
            val outputFolder: File = fsSinkFolder.listFiles.find(_.getName.contains("job1_1")).get
            val results = sparkSession.read.text(outputFolder.getPath)
            results.count() == 10
          }
        }
        if (!success) fail("Failed to find expected number of results. Either the results are not available or the number of results does not match")
      }
    }

    "rerun a job including a mapping" ignore { // TODO activate this test

      var firstId: Option[String] = Option.empty

      Get(s"/${webServerConfig.baseUri}/projects/${projectId}/jobs/${batchJob.id}/executions?page=1") ~> route ~> check {
        // Get id of previous execution

        val jValue = JsonMethods.parse(responseAs[String])

        firstId = (jValue.asInstanceOf[JArray].arr.head.asInstanceOf[JObject] \ "id").extractOpt[String]
      }

      // Rerun the previous job
      Post(s"/${webServerConfig.baseUri}/projects/${projectId}/jobs/${batchJob.id}/executions/${firstId.get}/run", HttpEntity(ContentTypes.`application/json`, "")) ~> route ~> check {
        status shouldEqual StatusCodes.OK

        // Mappings run asynchronously. Wait at most 10 seconds for mappings to complete.
        val success = waitForCondition(10) {
          fsSinkFolder.listFiles.exists(_.getName.contains("job1_1")) && {
            // Check the resources created in the file system
            val outputFolder: File = fsSinkFolder.listFiles.find(_.getName.contains("job1_1")).get
            val results = sparkSession.read.text(outputFolder.getPath)
            results.count() == 20
          }
        }
        if (!success) fail("Failed to find expected number of results. Either the results are not available or the number of results does not match")
      }
    }

    /**
     * This test ensures that mapping resources (i.e. mapping definitions, context maps, etc.) becomes available to execute even if they are created
     * after the server is up.
     *
     * Furthermore, the test also validates whether erroneous records are created appropriately
     */
    "run a job with a mapping and context map that are created after the server is up" in {
      // Create context map for the global project
      createContextMapAndVerify("other-observation-concept-map.csv", "other-observation-concept-map.csv")

      // Create a new mapping
      createMappingAndVerify("other-observation-mapping2.json", 1)

      // Update the job with the new mapping and new sink configuration
      val observationsMappingTask: FhirMappingTask = FhirMappingTask(
        mappingRef = "https://aiccelerate.eu/fhir/mappings/other-observation-mapping2",
        sourceContext = Map("source" -> FileSystemSource(path = "other-observations.csv"))
      )
      sinkSettings = FileSystemSinkSettings(path = s"./$fsSinkFolderName/job1_2", Some(SinkFileFormats.NDJSON))
      val job1Updated = batchJob.copy(mappings = Seq(observationsMappingTask), sinkSettings = sinkSettings, name = Some("updatedJob"))

      Put(s"/${webServerConfig.baseUri}/projects/${projectId}/jobs/$job1Id", HttpEntity(ContentTypes.`application/json`, writePretty(job1Updated))) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate the updated job
        val job: FhirMappingJob = JsonMethods.parse(responseAs[String]).extract[FhirMappingJob]
        job.name shouldEqual Some("updatedJob")
      }

      // Run the job
      Post(s"/${webServerConfig.baseUri}/projects/${projectId}/jobs/${batchJob.id}/run", HttpEntity(ContentTypes.`application/json`, "")) ~> route ~> check {
        status shouldEqual StatusCodes.OK

        // Mappings run asynchronously. Wait at most 10 seconds for mappings to complete.
        var success = waitForCondition(10) {
          fsSinkFolder.listFiles.exists(_.getName.contains("job1_2")) && {
            // Check the resources created in the file system
            val parquetFolder = fsSinkFolder.listFiles.find(_.getName.contains("job1_2")).get
            val results = sparkSession.read.text(parquetFolder.getPath)
            results.count() == 13
          }
        }
        if (!success) fail("Failed to find expected number of results. Either the results are not available or the number of results does not match")


        // test if erroneous records are written to error folder
        success = waitForCondition(10) {
          val erroneousRecordsFolder = Paths.get(toFhirEngineConfig.erroneousRecordsFolder, FhirMappingErrorCodes.MAPPING_ERROR)
          erroneousRecordsFolder.toFile.exists() && {
            val jobFolder = Paths.get(erroneousRecordsFolder.toString, s"job-${batchJob.id}").toFile
            val csvFile = jobFolder.listFiles().head.listFiles().head.listFiles().head
            csvFile.exists() && {
              val csvFileContent = sparkSession.read.option("header", "true").csv(csvFile.getPath)
              csvFileContent.count() == 1
            }
          }
        }
        if (!success) fail("Failed to find expected number of erroneous records. Either the erroneous record file is not available or the number of records does not match")
      }
    }

    sinkSettings = FileSystemSinkSettings(path = s"./$fsSinkFolderName/job2", Some(SinkFileFormats.NDJSON))
    val job2: FhirMappingJob = FhirMappingJob(name = Some("mappingJob2"), sourceSettings = dataSourceSettings, sinkSettings = sinkSettings, mappings = Seq(patientMappingTask), dataProcessingSettings = DataProcessingSettings())

    "execute a mapping that is included in the mapping task" in {
      // create the job
      Post(s"/${webServerConfig.baseUri}/projects/${projectId}/jobs", HttpEntity(ContentTypes.`application/json`, writePretty(job2))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that job metadata file is updated
        val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
        (projects.arr.find(p => (p \ "id").extract[String] == projectId).get \ "mappingJobs").asInstanceOf[JArray].arr.length shouldEqual 2
        // check job folder is created
        FileUtils.getPath(toFhirEngineConfig.jobRepositoryFolderPath, projectId, s"${job2.id}${FileExtensions.JSON}").toFile should exist
      }

      // Create the schema
      createSchemaAndVerify("patient-schema.json", 1)

      // Run mapping and verify results
      initializeTestMappingQuery(job2.id,
        "https://aiccelerate.eu/fhir/mappings/pilot1/patient-mapping",
        Map("source" -> FileSystemSource(path = "patients.csv")),
        Some(FileOperations.readJsonContentAsObject[FhirMapping](FileOperations.getFileIfExists(getClass.getResource("/patient-mapping.json").getPath)))) ~> check {

        status shouldEqual StatusCodes.OK
        val results: Seq[FhirMappingResult] = JsonMethods.parse(responseAs[String]).extract[Seq[FhirMappingResult]]
        results.length shouldEqual 3
        results.head.mappingUrl shouldEqual "https://aiccelerate.eu/fhir/mappings/pilot1/patient-mapping"
        results.head.mappedResource.get shouldEqual "{\"resourceType\":\"Patient\"," +
          "\"id\":\"34dc88d5972fd5472a942fc80f69f35c\"," +
          "\"meta\":{\"profile\":[\"https://aiccelerate.eu/fhir/StructureDefinition/AIC-Patient\"]," +
          "\"source\":\"https://aiccelerate.eu/data-integration-suite/test-data\"}," +
          "\"active\":true,\"identifier\":[{\"use\":\"official\",\"system\":\"https://aiccelerate.eu/data-integration-suite/test-data\",\"value\":\"p1\"}]," +
          "\"gender\":\"male\",\"birthDate\":\"2000-05-10\"}"
      }
    }

    "execute a mapping within a job without passing the mapping in the mapping task" in {
      // create the mapping that will be tested
      createMappingAndVerify("patient-mapping2.json", 2)

      initializeTestMappingQuery(job2.id, "https://aiccelerate.eu/fhir/mappings/pilot1/patient-mapping2", Map("source" -> FileSystemSource(path = "patients.csv"))) ~> check {
        status shouldEqual StatusCodes.OK
        val results: Seq[FhirMappingResult] = JsonMethods.parse(responseAs[String]).extract[Seq[FhirMappingResult]]
        results.length shouldEqual 3
        results.head.mappingUrl shouldEqual "https://aiccelerate.eu/fhir/mappings/pilot1/patient-mapping2"
        results.head.mappedResource.get shouldEqual "{\"resourceType\":\"Patient\"," +
          "\"id\":\"34dc88d5972fd5472a942fc80f69f35c\"," +
          "\"meta\":{\"profile\":[\"https://aiccelerate.eu/fhir/StructureDefinition/AIC-Patient\"]," +
          "\"source\":\"https://aiccelerate.eu/data-integration-suite/test-data\"}," +
          "\"active\":false,\"identifier\":[{\"use\":\"official\",\"system\":\"https://aiccelerate.eu/data-integration-suite/test-data\",\"value\":\"p1\"}]," +
          "\"gender\":\"male\",\"birthDate\":\"2000-05-10\"}"
      }
    }

    /**
     * This test aims to test a mapping, containing a reference to a concept map, can be executed when a mapping is created after the server is up.
     * The following activities are performed in the test:
     * 1) Source schema is created
     * 2) The mapping is created
     * 3) Mapping is run via the test endpoint
     */
    "execute a mapping with a context within a job" in {
      createSchemaAndVerify("other-observation-schema.json", 2)
      createMappingAndVerify("other-observation-mapping.json", 3)

      // test a mapping
      initializeTestMappingQuery(job2.id, "https://aiccelerate.eu/fhir/mappings/other-observation-mapping", Map("source" -> FileSystemSource(path = "other-observations.csv"))) ~> check {
        status shouldEqual StatusCodes.OK
        val results: Seq[FhirMappingResult] = JsonMethods.parse(responseAs[String]).extract[Seq[FhirMappingResult]]
        results.length shouldEqual 3
        results.head.mappingUrl shouldEqual "https://aiccelerate.eu/fhir/mappings/other-observation-mapping"

        val result: JObject = JsonMethods.parse(results.head.mappedResource.get).asInstanceOf[JObject]
        (result \ "meta" \ "profile").asInstanceOf[JArray].arr.head.extract[String] shouldEqual "https://aiccelerate.eu/fhir/StructureDefinition/AIC-IntraOperativeObservation"
        (result \ "effectiveDateTime").extract[String] startsWith "2007-10-12T10:00:00"
        (result \ "valueQuantity" \ "value").extract[Int] shouldEqual 450
      }
    }

    /**
     * Run streaming job with streaming data source folder, which contains some initial data
     * Then add data to the streaming data source folder and check if the streaming job is triggered
     */
    "run a streaming job including a mapping" in {
      // Create the streaming job
      Post(s"/${webServerConfig.baseUri}/projects/${projectId}/jobs", HttpEntity(ContentTypes.`application/json`, writePretty(streamingJob))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that job metadata file is updated
        val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
        (projects.arr.find(p => (p \ "id").extract[String] == projectId).get \ "mappingJobs").asInstanceOf[JArray].arr.length shouldEqual 3
        // check job folder is created
        FileUtils.getPath(toFhirEngineConfig.jobRepositoryFolderPath, projectId, s"${streamingJob.id}${FileExtensions.JSON}").toFile should exist
      }

      // Run the job
      Post(s"/${webServerConfig.baseUri}/projects/${projectId}/jobs/${streamingJob.id}/run", HttpEntity(ContentTypes.`application/json`, "")) ~> route ~> check {
        status shouldEqual StatusCodes.OK

        // Mappings run asynchronously. Wait at most 10 seconds for the job to finish
        val success = waitForCondition(10) {
          fsSinkFolder.listFiles.exists(_.getName.contains("job3")) && {
            // Check the resources created in the file system
            val outputFolder: File = fsSinkFolder.listFiles.find(_.getName.contains("job3")).get
            val results = sparkSession.read.text(outputFolder.getPath)
            results.count() == 10
          }
        }
        if (!success) fail("Failed to find expected number of results. Either the results are not available or the number of results does not match.")

        // put more data by copying same csv file with different name
        val csvFile = Paths.get(getClass.getResource(s"/$parentStreamingFolderName/$patientStreamingFolder/patients.csv").toURI).toFile
        // clone the csv file with different name
        val csvFile2 = new File(csvFile.getParentFile, "patients2.csv")
        org.apache.commons.io.FileUtils.copyFile(csvFile, csvFile2)
        // mappings run asynchronously, wait at most 10 seconds for the job to finish
        // check if the mapping is executed with cloned/new csv file automatically
        val success2 = waitForCondition(10) {
          fsSinkFolder.listFiles.exists(_.getName.contains("job3")) && {
            // Check the resources created in the file system
            val outputFolder: File = fsSinkFolder.listFiles.find(_.getName.contains("job3")).get
            val results = sparkSession.read.text(outputFolder.getPath)
            results.count() == 20
          }
        }
        if (!success2) fail("Failed to find empty sink file.")
      }
    }
  }

  /**
   * Deserializes a [[SchemaDefinition]] from the specified file. Submits the definition into the global project. Then verifies the results.
   *
   * @param schemaDefinitionResourceFile Name of the file to read the schema definition content
   * @param expectedSchemaCount          Expected number of schemas within global project
   */
  private def createSchemaAndVerify(schemaDefinitionResourceFile: String, expectedSchemaCount: Int): Unit = {
    val otherObservationSchemaFile: File = FileOperations.getFileIfExists(getClass.getResource(s"/$schemaDefinitionResourceFile").getPath)
    val otherObservationSourceSchema: SchemaDefinition = FileOperations.readJsonContentAsObject[SchemaDefinition](otherObservationSchemaFile)

    // create the schema that the mapping uses
    Post(s"/tofhir/projects/${projectId}/schemas", HttpEntity(ContentTypes.`application/json`, writePretty(otherObservationSourceSchema))) ~> route ~> check {
      status shouldEqual StatusCodes.Created
      // validate that schema metadata file is updated
      val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
      (projects.arr.find(p => (p \ "id").extract[String] == projectId).get \ "schemas").asInstanceOf[JArray].arr.length shouldEqual expectedSchemaCount
      // check schema folder is created
      FileUtils.getPath(toFhirEngineConfig.schemaRepositoryFolderPath, projectId, s"${otherObservationSourceSchema.id}${FileExtensions.StructureDefinition}${FileExtensions.JSON}").toFile should exist
    }
  }

  /**
   * First creates mapping context for the global project. Afterwards, the content of the concept map is read from the specified file and associated with the mapping context.
   *
   * @param conceptMapResourceFile File to read the content of the test concept map
   * @param mappingContextId       Name of the mapping context. This id is also used as the name of the file keeping the concept map as maintained by the repository.
   */
  private def createContextMapAndVerify(conceptMapResourceFile: String, mappingContextId: String): Unit = {
    // upload the concept map inside the project
    Post(s"/${webServerConfig.baseUri}/projects/${projectId}/mapping-contexts", HttpEntity(ContentTypes.`text/plain(UTF-8)`, mappingContextId)) ~> route ~> check {
      status shouldEqual StatusCodes.Created
      // validate that mapping context is updated in projects.json file
      val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
      (projects.arr.find(p => (p \ "id").extract[String] == projectId).get \ "mappingContexts").asInstanceOf[JArray].arr.length shouldEqual 1
      // check mapping context file is created
      FileUtils.getPath(toFhirEngineConfig.mappingContextRepositoryFolderPath, projectId, mappingContextId).toFile should exist
    }
    // get file from resources
    val file: File = FileOperations.getFileIfExists(getClass.getResource(s"/$conceptMapResourceFile").getPath)
    val fileData = Multipart.FormData.BodyPart.fromPath("attachment", ContentTypes.`text/plain(UTF-8)`, file.toPath)
    val formData = Multipart.FormData(fileData)
    // save a csv file to mapping context
    Post(s"/${webServerConfig.baseUri}/projects/${projectId}/mapping-contexts/$mappingContextId/content", formData.toEntity()) ~> route ~> check {
      status shouldEqual StatusCodes.OK
      responseAs[String] shouldEqual "OK"
    }
  }

  /**
   * Reads the mapping file from the file system into a [[FhirMapping]] instance. Posts the mapping into the global project and verifies that the mapping is inside the project folder.
   *
   * @param mappingResourceFile  Name of the file including the mapping definition
   * @param expectedMappingCount Number of mappings to be included in the global project
   */
  private def createMappingAndVerify(mappingResourceFile: String, expectedMappingCount: Int): Unit = {
    val mappingFile: File = FileOperations.getFileIfExists(getClass.getResource(s"/$mappingResourceFile").getPath)
    val mapping: FhirMapping = FileOperations.readJsonContentAsObject[FhirMapping](mappingFile)

    // create the mapping that will be tested
    Post(s"/${webServerConfig.baseUri}/projects/${projectId}/mappings", HttpEntity(ContentTypes.`application/json`, writePretty(mapping))) ~> route ~> check {
      status shouldEqual StatusCodes.Created
      // validate that mapping metadata file is updated
      val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
      (projects.arr.find(p => (p \ "id").extract[String] == projectId).get \ "mappings").asInstanceOf[JArray].arr.length shouldEqual expectedMappingCount
      // check mapping folder is created
      FileUtils.getPath(toFhirEngineConfig.mappingRepositoryFolderPath, projectId, s"${mapping.id}${FileExtensions.JSON}").toFile should exist
    }
  }

  /**
   * Creates a [[TestResourceCreationRequest]] with the given input and returns a [[RouteTestResult]] to be checked.
   *
   * @param jobId         Identifier of the job for which the test will be run
   * @param mappingRef    Url of the mapping to be tested
   * @param sourceContext A map of defining the sources from which the test data will be read e.g. Map("source" -> FileSystemSource(path = "patients.csv"))
   * @param mapping       Mapping itself to be tested. If the mapping itself is not provided, it is resolved via the [[mappingRef]]
   * @return
   */
  private def initializeTestMappingQuery(jobId: String, mappingRef: String, sourceContext: Map[String, FhirMappingSourceContext], mapping: Option[FhirMapping] = None): RouteTestResult = {
    val otherObservationsMappingTask: FhirMappingTask = FhirMappingTask(
      mappingRef = mappingRef,
      sourceContext = sourceContext,
      mapping
    )
    val createTestResourcesRequest: TestResourceCreationRequest = TestResourceCreationRequest(
      fhirMappingTask = otherObservationsMappingTask,
      resourceFilter = resourceFilter
    )

    // test a mapping
    Post(s"/${webServerConfig.baseUri}/projects/${projectId}/jobs/$jobId/test", HttpEntity(ContentTypes.`application/json`, writePretty(createTestResourcesRequest))) ~> route
  }

  /**
   * Define a function to wait for a condition with a timeout
   * @param timeoutSeconds timeout in seconds to wait for the condition
   * @param condition condition to be checked
   * @return
   */
  private def waitForCondition(timeoutSeconds: Int)(condition: => Boolean): Boolean = {
    var success = false
    breakable {
      for (_ <- 1 to timeoutSeconds) { // Sleep 1000ms up to `timeoutSeconds` times
        Thread.sleep(1000)
        if (condition) {
          success = true
          break
        }
      }
    }
    success
  }

  /**
   * Creates a project to be used in the tests
   * */
  override def beforeAll(): Unit = {
    super.beforeAll()
    org.apache.commons.io.FileUtils.deleteDirectory(new File(fsSinkFolderName))
    org.apache.commons.io.FileUtils.deleteDirectory(Paths.get(toFhirEngineConfig.erroneousRecordsFolder).toFile)
    fsSinkFolder.mkdirs()
    this.createProject(Some("deadbeef-dead-dead-dead-deaddeafbeef"))
  }

  override def afterAll(): Unit = {
    super.afterAll()
    org.apache.commons.io.FileUtils.deleteDirectory(fsSinkFolder)
    // delete checkpoint folder
    org.apache.commons.io.FileUtils.deleteDirectory(Paths.get(ToFhirConfig.sparkCheckpointDirectory).toFile)
    // delete erroneous folder
    org.apache.commons.io.FileUtils.deleteDirectory(Paths.get(toFhirEngineConfig.erroneousRecordsFolder).toFile)
    // remove cloned csv file used for streaming job
    org.apache.commons.io.FileUtils.delete(Paths.get(getClass.getResource(s"/$parentStreamingFolderName/$patientStreamingFolder/patients2.csv").toURI).toFile)
  }
}
