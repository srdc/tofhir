package io.onfhir.tofhir.engine

import akka.http.scaladsl.model.StatusCodes
import io.onfhir.client.OnFhirNetworkClient
import io.onfhir.tofhir.ToFhirTestSpec
import io.onfhir.tofhir.model.{FhirMappingTask, FhirRepositorySinkSettings, FileSystemSource, FileSystemSourceSettings, SourceFileFormats}
import io.onfhir.tofhir.util.FhirMappingUtility
import io.onfhir.util.JsonFormatter.formats
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.json4s.JsonAST.JArray

import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

class Pilot2IntegrationTest extends ToFhirTestSpec {

  val sparkConf:SparkConf = new SparkConf()
    .setAppName("tofhir-test")
    .setMaster("local[4]")
    .set("spark.driver.allowMultipleContexts", "false")
    .set("spark.ui.enabled", "false")

  val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  Paths.get(".") .toAbsolutePath

  val mappingRepository: IFhirMappingRepository =
    new FhirMappingFolderRepository(Paths.get("mappings/pilot2").toAbsolutePath.toUri)

  val contextLoader: IMappingContextLoader = new MappingContextLoader(mappingRepository)

  val schemaRepository = new SchemaFolderRepository(Paths.get("schemas/pilot2").toAbsolutePath.toUri)

  val dataSourceSettings: FileSystemSourceSettings = FileSystemSourceSettings("test-source-1", "http://hus.fi", Paths.get("test-data/pilot2").toAbsolutePath.toUri)

  val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, sparkSession)

  val fhirSinkSetting: FhirRepositorySinkSettings = FhirRepositorySinkSettings("http://localhost:8081/fhir")
  val onFhirClient = OnFhirNetworkClient.apply(fhirSinkSetting.fhirRepoUrl)

  val fhirServerIsAvailable =
    Try(Await.result(onFhirClient.search("Patient").execute(), FiniteDuration(5, TimeUnit.SECONDS)).httpStatus == StatusCodes.OK)
      .getOrElse(false)

  val patientMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot2/patient-mapping",
    sourceContext = Map("source" ->  FileSystemSource(path = "patients.csv", sourceType = SourceFileFormats.CSV, dataSourceSettings))
  )

  val encounterMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot2/encounter-mapping",
    sourceContext = Map("source" ->  FileSystemSource(path = "encounters.csv", sourceType = SourceFileFormats.CSV, dataSourceSettings))
  )

  val symptomsAssMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot2/symptom-assessment-mapping",
    sourceContext = Map("source" ->  FileSystemSource(path = "parkinson-symptom-assessments.csv", sourceType = SourceFileFormats.CSV, dataSourceSettings))
  )

  val symptomsExistenceMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot2/symptom-existence-mapping",
    sourceContext = Map("source" ->  FileSystemSource(path = "parkinson-symptom-existence.csv", sourceType = SourceFileFormats.CSV, dataSourceSettings))
  )

  "patient mapping" should "map test data" in {
    //Some semantic tests on generated content
    fhirMappingJobManager.executeMappingTaskAndReturn(task = patientMappingTask) map { results =>
      //10 patients and 4 motorSymptomsOnset
       results.length shouldBe 14

      val patients= results.filter(r => (r \ "meta" \ "profile").extract[Seq[String]].head == "https://aiccelerate.eu/fhir/StructureDefinition/AIC-Patient")
      (JArray(patients.toList) \ "id").extract[Seq[String]].toSet shouldBe (1 to 10).map(i => FhirMappingUtility.getHashedId("Patient", "p"+i)).toSet

      (JArray(patients.toList) \ "identifier" \ "value").extract[Seq[String]].toSet shouldBe (1 to 10).map(i => s"p$i").toSet
      (patients.apply(3) \ "gender").extract[String] shouldBe "male"
      (patients.apply(7) \ "gender").extract[String] shouldBe "female"
      (patients.apply(3) \ "birthDate").extract[String] shouldBe "1999-05-06"

      val conditions = results.filter(r => (r \ "meta" \ "profile").extract[Seq[String]].head == "https://aiccelerate.eu/fhir/StructureDefinition/AIC-PatientReportedCondition")
     (JArray(conditions.toList) \ "subject" \"reference").extract[Seq[String]].toSet shouldBe Set("p2", "p3", "p4", "p7").map(p => FhirMappingUtility.getHashedReference("Patient", p))
    }
  }

  it should "map test data and write it to FHIR repo successfully" in {
    //Send it to our fhir repo if they are also validated
    assert(fhirServerIsAvailable)
    fhirMappingJobManager
      .executeMappingJob(tasks = Seq(patientMappingTask), sinkSettings = fhirSinkSetting)
      .map( unit =>
        unit shouldBe ()
      )
  }

  "encounter mapping" should "map test data" in {
    //Some semantic tests on generated content
    fhirMappingJobManager.executeMappingTaskAndReturn(task = encounterMappingTask) map { results =>
      results.length shouldBe 2
      (results.head \ "subject" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Patient", "p1")
      (results.last \ "id").extract[String] shouldBe FhirMappingUtility.getHashedId("Encounter", "e2")
      (results.last \ "class" \ "code").extract[String] shouldBe "EMER"
      (results.last \ "class" \ "display").extract[String] shouldBe "Emergency visit"
      (results.head \ "type" \ "coding" \ "code").extract[Seq[String]].head shouldBe "225398001"
      (results.head \ "type" \ "coding" \ "display").extract[Seq[String]].head shouldBe "Neurological Assessment"
      (results.head \ "period" \ "start").extract[String] shouldBe "2012-08-23"
    }
  }

  it should "map test data and write it to FHIR repo successfully" in {
    //Send it to our fhir repo if they are also validated
    assert(fhirServerIsAvailable)
    fhirMappingJobManager
      .executeMappingJob(tasks = Seq(encounterMappingTask), sinkSettings = fhirSinkSetting)
      .map( unit =>
        unit shouldBe ()
      )
  }

  "symptom assessment mapping" should "map test data" in {
    //Some semantic tests on generated content
    fhirMappingJobManager.executeMappingTaskAndReturn(task = symptomsAssMappingTask) map { results =>
      results.length shouldBe 12
      (results.apply(2) \ "subject" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Patient", "p1")
      (results.apply(2) \ "encounter" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Encounter", "e1")
      (results.apply(2) \ "code" \ "coding" \ "code").extract[Seq[String]].head shouldBe "271587009"
      (results.apply(2) \ "code" \ "coding" \ "display").extract[Seq[String]].head shouldBe "Stiffness, rigidity"
      (results.apply(2) \ "meta" \ "profile").extract[Seq[String]].head shouldBe "https://aiccelerate.eu/fhir/StructureDefinition/AIC-ParkinsonStiffnessScore"
      (results.apply(2) \ "method" \ "coding" \ "code").extract[Seq[String]].head shouldBe "updrs3"
      (results.apply(2) \ "method" \ "coding" \ "display").extract[Seq[String]].head shouldBe "UPDRS v3 Questionnaire"
      (results.apply(5) \ "effectivePeriod" \ "start").extract[String] shouldBe "2012-02-07"
      (results.apply(5) \ "effectivePeriod" \ "end").extract[String] shouldBe "2012-02-13"
    }
  }

  it should "map test data and write it to FHIR repo successfully" in {
    //Send it to our fhir repo if they are also validated
    assert(fhirServerIsAvailable)
    fhirMappingJobManager
      .executeMappingJob(tasks = Seq(symptomsAssMappingTask), sinkSettings = fhirSinkSetting)
      .map( unit =>
        unit shouldBe ()
      )
  }

  "symptom existence mapping" should "map test data" in {
    //Some semantic tests on generated content
    fhirMappingJobManager.executeMappingTaskAndReturn(task = symptomsExistenceMappingTask) map { results =>
      results.length shouldBe 6
      (results.head \ "identifier" \ "value").extract[Seq[String]] shouldBe Seq("se1")
      (results.apply(1) \ "subject" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Patient", "p1")
      (results.apply(1) \ "encounter" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Encounter", "e1")

      (results.head \ "code" \ "coding" \ "code").extract[Seq[String]].head shouldBe "443544006"
      (results.head \ "code" \ "coding" \ "display").extract[Seq[String]].head shouldBe "Freezing of gait"
      (results.apply(4) \ "valueBoolean").extract[Boolean] shouldBe(true)
      (results.last \ "code" \ "coding" \ "code").extract[Seq[String]].head shouldBe "39898005"
    }
  }

  it should "map test data and write it to FHIR repo successfully" in {
    //Send it to our fhir repo if they are also validated
    assert(fhirServerIsAvailable)
    fhirMappingJobManager
      .executeMappingJob(tasks = Seq(symptomsExistenceMappingTask), sinkSettings = fhirSinkSetting)
      .map( unit =>
        unit shouldBe ()
      )
  }



}
