package io.tofhir.test

import akka.http.scaladsl.model.StatusCodes
import io.onfhir.api.Resource
import io.onfhir.api.client.FhirBatchTransactionRequestBuilder
import io.onfhir.path.{FhirPathAggFunctionsFactory, FhirPathUtilFunctionsFactory}
import io.onfhir.util.JsonFormatter._
import io.tofhir.engine.data.write.FileSystemWriter.SinkContentTypes
import io.tofhir.engine.mapping.job.FhirMappingJobManager
import io.tofhir.engine.model._
import io.tofhir.{OnFhirTestContainer, ToFhirTestSpec}
import org.json4s.JValue
import org.json4s.jackson.JsonMethods
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AsyncFlatSpec

import java.io.File
import scala.io.Source

/**
 * Test suite for verifying the behavior of FhirServerSource.
 */
class FhirServerSourceTest extends AsyncFlatSpec with BeforeAndAfterAll with ToFhirTestSpec with OnFhirTestContainer {

  // Sink Settings of mapping job
  val fhirSinkSettings: FhirRepositorySinkSettings = FhirRepositorySinkSettings(fhirRepoUrl = onFhirClient.getBaseUrl())

  // Settings of Fhir Server data source
  val fhirServerSourceSettings: Map[String, FhirServerSourceSettings] =
    Map(
      "source" ->
        FhirServerSourceSettings(name = "test-fhir-server-source", sourceUri = "https://map-from-fhir-test", serverUrl = onFhirClient.getBaseUrl()))

  // FhirMappingJobManager to execute mapping job
  val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository,
    Map(FhirPathUtilFunctionsFactory.defaultPrefix -> FhirPathUtilFunctionsFactory, FhirPathAggFunctionsFactory.defaultPrefix -> FhirPathAggFunctionsFactory),
    sparkSession)

  val patientMappingTask: FhirMappingTask = FhirMappingTask(
    name = "patient-fhir-mapping",
    mappingRef = "https://datatools4heart.eu/fhir/mappings/patient-fhir-mapping",
    sourceBinding = Map("source" -> FhirServerSource(resourceType = "Patient"))
  )

  val patientMappingTaskWith2Sources: FhirMappingTask = FhirMappingTask(
    name = "patient-fhir-mapping-two-sources",
    mappingRef = "https://datatools4heart.eu/fhir/mappings/patient-fhir-mapping-two-sources",
    sourceBinding = Map("patient" -> FhirServerSource(resourceType = "Patient"), "observations" -> FhirServerSource(resourceType = "Observation"))
  )

  // Observation mapping task
  val observationMappingTaskWith2Sources: FhirMappingTask = FhirMappingTask(
    name = "observation-mapping-from-fhir-with-two-sources",
    mappingRef = "http://observation-mapping-from-fhir-with-two-sources",
    sourceBinding = Map("observation" -> FhirServerSource(resourceType = "Observation"), "patient" -> FhirServerSource(resourceType = "Patient")))

  // Mapping Job
  val fhirMappingJob: FhirMappingJob = FhirMappingJob(
    name = Some("test-fhir-server-mappingjob"),
    mappings = Seq.empty,
    sourceSettings = fhirServerSourceSettings,
    sinkSettings = fhirSinkSettings,
    dataProcessingSettings = DataProcessingSettings()
  )

  // The folder where the generated resources will be written
  val fsSinkFolder: File = new File("fsSink")

  // Resources to be created on onFhir server as the source data
  val testPatientResource: Resource = JsonMethods.parse(Source.fromInputStream(getClass.getResourceAsStream("/fhir-resources/patient-resource.json")).mkString).extract[Resource]
  val testObservationResource1: Resource = JsonMethods.parse(Source.fromInputStream(getClass.getResourceAsStream("/fhir-resources/observation-resource-1.json")).mkString).extract[Resource]
  val testObservationResource2: Resource = JsonMethods.parse(Source.fromInputStream(getClass.getResourceAsStream("/fhir-resources/observation-resource-2.json")).mkString).extract[Resource]
  val testObservationResource3: Resource = JsonMethods.parse(Source.fromInputStream(getClass.getResourceAsStream("/fhir-resources/observation-resource-3.json")).mkString).extract[Resource]
  val encounterSummaryMappingResources: Seq[Resource] = JsonMethods.parse(Source.fromInputStream(getClass.getResourceAsStream("/fhir-resources/encounter-summary-mapping-resource.json")).mkString).extract[Seq[Resource]]

  /**
   * Create the resources on the onFHIR server before starting the tests.
   * */
  override protected def beforeAll(): Unit = {
    super.beforeAll()
    var batchRequest = onFhirClient.batch()
      .entry(_.update(testPatientResource))
      .entry(_.update(testObservationResource1))
      .entry(_.update(testObservationResource2))
      .entry(_.update(testObservationResource3))
    // add FHIR Resources for encounter-summary mapping
    encounterSummaryMappingResources.foreach(r => {
      batchRequest = batchRequest.entry(_.update(r))
    })
    batchRequest
      .returnMinimal().asInstanceOf[FhirBatchTransactionRequestBuilder].execute() map { res =>
      res.httpStatus shouldBe StatusCodes.OK
    }
  }

  /**
   * After the tests complete, delete the resources from onFHIR server.
   * */
  override protected def afterAll(): Unit = {
    super.afterAll()
    // delete test resources on onFHIR
    onFhirClient.batch()
      .entry(_.delete("Patient"))
      .entry(_.delete("Condition"))
      .entry(_.delete("Observation"))
      .entry(_.delete("Encounter"))
      .returnMinimal().asInstanceOf[FhirBatchTransactionRequestBuilder].execute() map { res =>
      res.httpStatus shouldBe StatusCodes.OK
    }
    // delete the file system sink folder
    org.apache.commons.io.FileUtils.deleteDirectory(fsSinkFolder)
  }

  it should "map Encounter, Observation, Condition and Patient FHIR data to a flat structure and write it to a CSV file successfully" in {
    val job: FhirMappingJob = FhirMappingJob(
      name = Some("encounter-summary-job"),
      mappings = Seq.empty,
      sourceSettings = Map(
        "fhirServer" -> FhirServerSourceSettings(name = "fhirServer", sourceUri = "http://fhir-server-source", serverUrl = onFhirClient.getBaseUrl())
      ),
      sinkSettings = FileSystemSinkSettings(path = s"${fsSinkFolder.getPath}/results.csv", contentType= SinkContentTypes.CSV, options = Map("header" -> "true")),
      dataProcessingSettings = DataProcessingSettings()
    )
    val mappingTask: FhirMappingTask = FhirMappingTask(
      name ="encounter-summary",
      mappingRef = "http://encounter-summary",
      sourceBinding = Map(
        "encounter" -> FhirServerSource(resourceType = "Encounter", sourceRef = Some("fhirServer")),
        "condition" -> FhirServerSource(resourceType = "Condition", sourceRef = Some("fhirServer")),
        "patient" -> FhirServerSource(resourceType = "Patient", sourceRef = Some("fhirServer")),
        "observation" -> FhirServerSource(resourceType = "Observation", sourceRef = Some("fhirServer"))
      )
    )

    fhirMappingJobManager.executeMappingJob(
      mappingJobExecution = FhirMappingJobExecution(mappingTasks = Seq(mappingTask), job = job),
      sourceSettings = job.sourceSettings,
      sinkSettings = job.sinkSettings
    ) map { _ =>
      // read the csv file created in the file system
      val csvFile: File = fsSinkFolder.listFiles.find(_.getName.contains("results.csv"))
        .get.listFiles().find(file => file.getName.endsWith(".csv"))
        .get
      val results = sparkSession.read
        .option("header", "true")
        .csv(csvFile.getPath)
      // verify the row count
      results.count() == 2
      // verify the content of rows
      val firstRow = results.head()
      firstRow.getAs[String]("encounterId") shouldEqual ("example-encounter")
      firstRow.getAs[String]("patientId") shouldEqual ("example")
      firstRow.getAs[String]("gender") shouldEqual ("male")
      firstRow.getAs[String]("birthDate") shouldEqual "1974-12-25"
      firstRow.getAs[String]("encounterClass") shouldEqual ("AMB")
      firstRow.getAs[String]("encounterStart") shouldEqual ("2024-07-22T08:00:00Z")
      firstRow.getAs[String]("encounterEnd") shouldEqual ("2024-07-22T09:00:00Z")
      firstRow.getAs[String]("observationLoincCode") shouldEqual ("2093-3")
      firstRow.getAs[String]("observationDate") shouldEqual ("2024-07-22T09:00:00Z")
      firstRow.getAs[String]("observationResult") shouldEqual ("37.5 Celsius")
      firstRow.getAs[String]("conditionSnomedCode") shouldEqual ("44054006")
      firstRow.getAs[String]("conditionDate") shouldEqual ("2024-07-22")
      val secondRow = results.limit(2).collect()(1)
      secondRow.getAs[String]("encounterId") shouldEqual "example-encounter-2"
      secondRow.getAs[String]("patientId") shouldEqual "example"
      secondRow.getAs[String]("gender") shouldEqual "male"
      secondRow.getAs[String]("birthDate") shouldEqual "1974-12-25"
      secondRow.getAs[String]("encounterClass") shouldEqual "EMER"
      secondRow.getAs[String]("encounterStart") shouldEqual "2024-08-01T10:00:00Z"
      secondRow.getAs[String]("encounterEnd") shouldEqual "2024-08-02T09:00:00Z"
      secondRow.getAs[String]("observationLoincCode") shouldEqual "85354-9"
      secondRow.getAs[String]("observationDate") shouldEqual "2024-08-01T10:15:00Z"
      secondRow.getAs[String]("observationResult") shouldEqual "140 mmHg"
      secondRow.getAs[String]("conditionSnomedCode") shouldEqual "38341003"
      secondRow.getAs[String]("conditionDate") shouldEqual "2024-08-01"
    }
  }

  it should "read data from FHIR server and execute the Patient mapping with a single source" in {
    fhirMappingJobManager.executeMappingTaskAndReturn(
      mappingJobExecution = FhirMappingJobExecution(mappingTasks = Seq(patientMappingTask), job = fhirMappingJob),
      mappingJobSourceSettings = fhirServerSourceSettings
    ) map { mappingResults =>
      mappingResults.length shouldBe 2
      val patientResource = mappingResults.head.mappedFhirResource.get.mappedResource.get.parseJson
      patientResource shouldBe a[Resource]
      (patientResource \ "meta" \ "profile").extract[Seq[String]].head shouldBe "https://datatools4heart.eu/fhir/StructureDefinition/HFR-Patient"
      (patientResource \ "active").extract[Boolean] shouldBe true
      (patientResource \ "identifier" \ "system").extract[Seq[String]].head shouldBe "https://map-from-fhir-test"
    }
  }

  it should "execute Patient mapping which joins Observation resources to Patient resources" in {
    fhirMappingJobManager.executeMappingTaskAndReturn(
      mappingJobExecution = FhirMappingJobExecution(mappingTasks = Seq(patientMappingTaskWith2Sources), job = fhirMappingJob),
      mappingJobSourceSettings = fhirServerSourceSettings
    ) map { mappingResults =>
      mappingResults.length shouldBe 2
      val patientResource = mappingResults.last.mappedFhirResource.get.mappedResource.get.parseJson
      (patientResource \ "identifier" \ "value").extract[Seq[String]].head shouldBe "8216"
      val extensionList = (patientResource \ "extension").extract[Seq[JValue]]
      val tempSum = extensionList.find(e => (e \ "url").extract[String] == "https://map-from-fhir-test/temp-sum")
      tempSum.isDefined shouldBe true
      (tempSum.get \ "valueQuantity").extract[Double] shouldBe 77.0
      val tempAvg = extensionList.find(e => (e \ "url").extract[String] == "https://map-from-fhir-test/temp-avg")
      tempAvg.isDefined shouldBe true
      (tempAvg.get \ "valueQuantity").extract[Double] shouldBe 38.5
      val respMaxExt = extensionList.find(e => (e \ "url").extract[String] == "https://map-from-fhir-test/resp-max")
      respMaxExt.isDefined shouldBe true
      (respMaxExt.get \ "valueQuantity").extract[Double] shouldBe 24
    }
  }

  it should "execute Observation mapping which joins Patient resources to Observation resources" in {
    fhirMappingJobManager.executeMappingTaskAndReturn(
      mappingJobExecution = FhirMappingJobExecution(mappingTasks = Seq(observationMappingTaskWith2Sources), job = fhirMappingJob),
      mappingJobSourceSettings = fhirServerSourceSettings
    ) map { mappingResults =>
      mappingResults.length shouldBe 5
      val observationResources = mappingResults.map(r => r.mappedFhirResource.get.mappedResource.get.parseJson)
      val joinedPatientObservations = observationResources.filter(r => (r \ "subject" \ "reference").extract[String] == "Patient/example-patient-fhir")
      joinedPatientObservations.length shouldBe 2
      var extensionList = (joinedPatientObservations.head \ "extension").extract[Seq[JValue]]
      var patientGender = extensionList.find(e => (e \ "url").extract[String] == "https://map-from-fhir-test/patient-gender")
      patientGender.isDefined shouldBe true
      (patientGender.get \ "valueString").extract[String] shouldBe "female"
      val patientBirthDate = extensionList.find(e => (e \ "url").extract[String] == "https://map-from-fhir-test/patient-birthdate")
      (patientBirthDate.get \ "valueString").extract[String] shouldBe "2011-04-01"
      val nonExistentPatientsObservation = observationResources.filterNot(r => (r \ "subject" \ "reference").extract[String] == "Patient/example-patient-fhir")
      nonExistentPatientsObservation.length shouldBe 3
      extensionList = (nonExistentPatientsObservation.head \ "extension").extract[Seq[JValue]]
      patientGender = extensionList.find(e => (e \ "url").extract[String] == "https://map-from-fhir-test/patient-gender")
      patientGender.isDefined shouldBe true
      (patientGender.get \ "valueString").extract[String] shouldBe "male"
    }
  }

  it should "map test data and write it to FHIR repo successfully" in {
    fhirMappingJobManager.executeMappingJob(
      mappingJobExecution = FhirMappingJobExecution(mappingTasks = Seq(patientMappingTaskWith2Sources, observationMappingTaskWith2Sources), job = fhirMappingJob),
      sourceSettings = fhirServerSourceSettings,
      sinkSettings = fhirSinkSettings
    ) flatMap { _ =>
      onFhirClient.search("Patient").where("_id", "example-patient-fhir").executeAndReturnBundle() map { bundle =>
        bundle.searchResults.size shouldBe 1
        val patientResource = bundle.searchResults.head
        val extensionList = (patientResource \ "extension").extract[Seq[JValue]]
        val tempSum = extensionList.find(e => (e \ "url").extract[String] == "https://map-from-fhir-test/temp-sum")
        tempSum.isDefined shouldBe true
        (tempSum.get \ "valueQuantity").extract[Double] shouldBe 77.0
        val tempAvg = extensionList.find(e => (e \ "url").extract[String] == "https://map-from-fhir-test/temp-avg")
        tempAvg.isDefined shouldBe true
        (tempAvg.get \ "valueQuantity").extract[Double] shouldBe 38.5
        val respMaxExt = extensionList.find(e => (e \ "url").extract[String] == "https://map-from-fhir-test/resp-max")
        respMaxExt.isDefined shouldBe true
        (respMaxExt.get \ "valueQuantity").extract[Double] shouldBe 24
      }
    }
  }

}

