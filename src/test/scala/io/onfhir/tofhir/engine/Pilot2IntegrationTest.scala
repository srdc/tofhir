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

}
