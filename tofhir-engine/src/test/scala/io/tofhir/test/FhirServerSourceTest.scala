package io.tofhir.test

import akka.http.scaladsl.model.StatusCodes
import io.onfhir.api.Resource
import io.onfhir.api.client.FhirBatchTransactionRequestBuilder
import io.onfhir.path.{FhirPathAggFunctionsFactory, FhirPathUtilFunctionsFactory}
import io.onfhir.util.JsonFormatter._
import io.tofhir.engine.mapping.job.FhirMappingJobManager
import io.tofhir.engine.model._
import io.tofhir.{OnFhirTestContainer, ToFhirTestSpec}
import org.json4s.JValue
import org.json4s.jackson.JsonMethods
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AsyncFlatSpec

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
    mappingRef = "https://datatools4heart.eu/fhir/mappings/patient-fhir-mapping",
    sourceContext = Map("source" -> FhirServerSource(resourceType = "Patient"))
  )

  val patientMappingTaskWith2Sources: FhirMappingTask = FhirMappingTask(
    mappingRef = "https://datatools4heart.eu/fhir/mappings/patient-fhir-mapping-two-sources",
    sourceContext = Map("patient" -> FhirServerSource(resourceType = "Patient"), "observations" -> FhirServerSource(resourceType = "Observation"))
  )

  // Observation mapping task
  val observationMappingTaskWith2Sources: FhirMappingTask = FhirMappingTask(
    mappingRef = "http://observation-mapping-from-fhir-with-two-sources",
    sourceContext = Map("observation" -> FhirServerSource(resourceType = "Observation"), "patient" -> FhirServerSource(resourceType = "Patient")))

  // Mapping Job
  val fhirMappingJob: FhirMappingJob = FhirMappingJob(
    name = Some("test-fhir-server-mappingjob"),
    mappings = Seq.empty,
    sourceSettings = fhirServerSourceSettings,
    sinkSettings = fhirSinkSettings,
    dataProcessingSettings = DataProcessingSettings()
  )

  // Resources to be created on onFhir server as the source data
  val testPatientResource: Resource = JsonMethods.parse(Source.fromInputStream(getClass.getResourceAsStream("/fhir-resources/patient-resource.json")).mkString).extract[Resource]
  val testObservationResource1: Resource = JsonMethods.parse(Source.fromInputStream(getClass.getResourceAsStream("/fhir-resources/observation-resource-1.json")).mkString).extract[Resource]
  val testObservationResource2: Resource = JsonMethods.parse(Source.fromInputStream(getClass.getResourceAsStream("/fhir-resources/observation-resource-2.json")).mkString).extract[Resource]
  val testObservationResource3: Resource = JsonMethods.parse(Source.fromInputStream(getClass.getResourceAsStream("/fhir-resources/observation-resource-3.json")).mkString).extract[Resource]

  /**
   * Create the resources on the onFHIR server before starting the tests.
   * */
  override protected def beforeAll(): Unit = {
    super.beforeAll()
    onFhirClient.batch()
      .entry(_.update(testPatientResource))
      .entry(_.update(testObservationResource1))
      .entry(_.update(testObservationResource2))
      .entry(_.update(testObservationResource3))
      .returnMinimal().asInstanceOf[FhirBatchTransactionRequestBuilder].execute() map { res =>
      res.httpStatus shouldBe StatusCodes.OK
    }
  }

  /**
   * After the tests complete, delete the resources from onFHIR server.
   * */
  override protected def afterAll(): Unit = {
    super.afterAll()
    onFhirClient.batch()
      .entry(_.delete("Observation", "example-patient-fhir"))
      .entry(_.delete("Observation", "example-observation-fhir-1"))
      .entry(_.delete("Observation", "example-observation-fhir-2"))
      .entry(_.delete("Observation", "example-observation-fhir-3"))
      .returnMinimal().asInstanceOf[FhirBatchTransactionRequestBuilder].execute() map { res =>
      res.httpStatus shouldBe StatusCodes.OK
    }
  }

  it should "read data from FHIR server and execute the Patient mapping with a single source" in {
    fhirMappingJobManager.executeMappingTaskAndReturn(
      mappingJobExecution = FhirMappingJobExecution(mappingTasks = Seq(patientMappingTask), job = fhirMappingJob),
      sourceSettings = fhirServerSourceSettings
    ) map { mappingResults =>
      mappingResults.length shouldBe 1
      val patientResource = mappingResults.head.mappedResource.get.parseJson
      patientResource shouldBe a[Resource]
      (patientResource \ "meta" \ "profile").extract[Seq[String]].head shouldBe "https://datatools4heart.eu/fhir/StructureDefinition/HFR-Patient"
      (patientResource \ "active").extract[Boolean] shouldBe true
      (patientResource \ "identifier" \ "system").extract[Seq[String]].head shouldBe "https://map-from-fhir-test"
    }
  }

  it should "execute Patient mapping which joins Observation resources to Patient resources" in {
    fhirMappingJobManager.executeMappingTaskAndReturn(
      mappingJobExecution = FhirMappingJobExecution(mappingTasks = Seq(patientMappingTaskWith2Sources), job = fhirMappingJob),
      sourceSettings = fhirServerSourceSettings
    ) map { mappingResults =>
      mappingResults.length shouldBe 1
      val patientResource = mappingResults.head.mappedResource.get.parseJson
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
      sourceSettings = fhirServerSourceSettings
    ) map { mappingResults =>
      mappingResults.length shouldBe 3
      val observationResources = mappingResults.map(r => r.mappedResource.get.parseJson)
      val joinedPatientObservations = observationResources.filter(r => (r \ "subject" \ "reference").extract[String] == "Patient/example-patient-fhir")
      joinedPatientObservations.length shouldBe 2
      var extensionList = (joinedPatientObservations.head \ "extension").extract[Seq[JValue]]
      var patientGender = extensionList.find(e => (e \ "url").extract[String] == "https://map-from-fhir-test/patient-gender")
      patientGender.isDefined shouldBe true
      (patientGender.get \ "valueString").extract[String] shouldBe "female"
      val patientBirthDate = extensionList.find(e => (e \ "url").extract[String] == "https://map-from-fhir-test/patient-birthdate")
      (patientBirthDate.get \ "valueString").extract[String] shouldBe "2011-04-01"
      val nonExistentPatientsObservation = observationResources.filterNot(r => (r \ "subject" \ "reference").extract[String] == "Patient/example-patient-fhir")
      nonExistentPatientsObservation.length shouldBe 1
      extensionList = (nonExistentPatientsObservation.head \ "extension").extract[Seq[JValue]]
      patientGender = extensionList.find(e => (e \ "url").extract[String] == "https://map-from-fhir-test/patient-gender")
      patientGender.isDefined shouldBe true
      (patientGender.get \ "valueString").extractOpt[String] shouldBe None
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

