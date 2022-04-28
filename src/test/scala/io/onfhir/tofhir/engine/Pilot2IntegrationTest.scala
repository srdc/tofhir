package io.onfhir.tofhir.engine

import akka.http.scaladsl.model.StatusCodes
import io.onfhir.client.OnFhirNetworkClient
import io.onfhir.tofhir.ToFhirTestSpec
import io.onfhir.tofhir.config.{MappingErrorHandling, ToFhirConfig}
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

  override val mappingRepository: IFhirMappingRepository =
    new FhirMappingFolderRepository(Paths.get("mappings/pilot2").toAbsolutePath.toUri)

  override val contextLoader: IMappingContextLoader = new MappingContextLoader(mappingRepository)

  override val schemaRepository = new SchemaFolderRepository(Paths.get("schemas/pilot2").toAbsolutePath.toUri)

  val dataSourceSettings: FileSystemSourceSettings = FileSystemSourceSettings("test-source-1", "http://hus.fi", Paths.get("test-data/pilot2").toAbsolutePath.toString)

  val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, sparkSession, MappingErrorHandling.withName(ToFhirConfig.mappingErrorHandling))

  val fhirSinkSetting: FhirRepositorySinkSettings = FhirRepositorySinkSettings(fhirRepoUrl = "http://localhost:8081/fhir", writeErrorHandling = MappingErrorHandling.CONTINUE)
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

  val otherAssMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot2/other-assessments-mapping",
    sourceContext = Map("source" ->  FileSystemSource(path = "other-assessments.csv", sourceType = SourceFileFormats.CSV, dataSourceSettings))
  )

  val conditionMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot2/condition-mapping",
    sourceContext = Map("source" ->  FileSystemSource(path = "conditions.csv", sourceType = SourceFileFormats.CSV, dataSourceSettings))
  )

  val medicationUsedMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot2/medication-used-mapping",
    sourceContext = Map("source" ->  FileSystemSource(path = "medication-used.csv", sourceType = SourceFileFormats.CSV, dataSourceSettings))
  )

  val deviceUsedMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot2/device-used-mapping",
    sourceContext = Map("source" ->  FileSystemSource(path = "device-used.csv", sourceType = SourceFileFormats.CSV, dataSourceSettings))
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
    assume(fhirServerIsAvailable)
    fhirMappingJobManager
      .executeMappingJob(tasks = Seq(patientMappingTask), sinkSettings = fhirSinkSetting)
      .map( unit =>
        unit shouldBe ()
      )
  }

  "encounter mapping" should "map test data" in {
    //Some semantic tests on generated content
    fhirMappingJobManager.executeMappingTaskAndReturn(task = encounterMappingTask) map { results =>
      results.length shouldBe 3
      (results.head \ "subject" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Patient", "p1")
      (results.apply(1) \ "id").extract[String] shouldBe FhirMappingUtility.getHashedId("Encounter", "e2")
      (results.apply(1) \ "class" \ "code").extract[String] shouldBe "EMER"
      (results.apply(1) \ "class" \ "display").extract[String] shouldBe "Emergency visit"
      (results.head \ "type" \ "coding" \ "code").extract[Seq[String]].head shouldBe "225398001"
      (results.head \ "type" \ "coding" \ "display").extract[Seq[String]].head shouldBe "Neurological Assessment"
      (results.head \ "period" \ "start").extract[String] shouldBe "2012-08-23"
    }
  }

  it should "map test data and write it to FHIR repo successfully" in {
    //Send it to our fhir repo if they are also validated
    assume(fhirServerIsAvailable)
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
    assume(fhirServerIsAvailable)
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
    assume(fhirServerIsAvailable)
    fhirMappingJobManager
      .executeMappingJob(tasks = Seq(symptomsExistenceMappingTask), sinkSettings = fhirSinkSetting)
      .map( unit =>
        unit shouldBe ()
      )
  }

  "other assessment mapping" should "map test data" in {
    //Some semantic tests on generated content
    fhirMappingJobManager.executeMappingTaskAndReturn(task = otherAssMappingTask) map { results =>
      results.length shouldBe 5
      (results.head \ "identifier" \ "value").extract[Seq[String]] shouldBe Seq("oo1")
      (results.apply(1) \ "subject" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Patient", "p1")
      (results.apply(1) \ "encounter" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Encounter", "e1")
      (results.apply(2) \ "code" \ "coding" \ "code").extract[Seq[String]].head shouldBe "bis-11"
      (results.apply(2) \ "code" \ "coding" \ "display").extract[Seq[String]].head shouldBe "Barratt Impulsiveness Scale-11 score"
      (results.apply(3) \ "code" \ "coding" \ "code").extract[Seq[String]].head shouldBe "tmt-a"
      (results.apply(3) \ "valueQuantity" \ "code").extract[String] shouldBe "{Zscore}"
    }
  }

  it should "map test data and write it to FHIR repo successfully" in {
    //Send it to our fhir repo if they are also validated
    assume(fhirServerIsAvailable)
    fhirMappingJobManager
      .executeMappingJob(tasks = Seq(otherAssMappingTask), sinkSettings = fhirSinkSetting)
      .map( unit =>
        unit shouldBe ()
      )
  }

  "condition mapping" should "map test data" in {
    //Some semantic tests on generated content
    fhirMappingJobManager.executeMappingTaskAndReturn(task = conditionMappingTask) map { results =>
       results.length shouldBe 5

      (results.apply(1) \ "subject" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Patient", "p2")
      (results.apply(1) \ "encounter" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Encounter", "e2")
      (results.apply(2) \ "code" \ "coding" \ "code").extract[Seq[String]].head shouldBe "J85"
      (results.apply(2) \ "code" \ "coding" \ "display").extract[Seq[String]].head shouldBe "Abscess of lung and mediastinum"

      (results.head \ "onsetDateTime" ).extract[String] shouldBe "2010-10-15"
      (results.head \ "abatementDateTime" ).extract[String] shouldBe "2010-11-15"
    }
  }

  it should "map test data and write it to FHIR repo successfully" in {
    //Send it to our fhir repo if they are also validated
    assume(fhirServerIsAvailable)
    fhirMappingJobManager
      .executeMappingJob(tasks = Seq(conditionMappingTask), sinkSettings = fhirSinkSetting)
      .map( unit =>
        unit shouldBe ()
      )
  }

  "medication used mapping" should "map test data" in {
    //Some semantic tests on generated content
    fhirMappingJobManager.executeMappingTaskAndReturn(task = medicationUsedMappingTask) map { results =>
      results.length shouldBe 5
      (results.apply(1) \ "subject" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Patient", "p2")
      (results.apply(2) \ "medicationCodeableConcept" \ "coding" \ "code").extract[Seq[String]].head shouldBe "N06AB03"
      (results.apply(2) \ "medicationCodeableConcept" \ "coding" \ "display").extract[Seq[String]].head shouldBe "fluoxetine"

      (results.apply(1) \ "dosage" \"timing" \ "repeat" \ "frequency").extract[Seq[Int]] shouldBe Seq(2)
      (results.apply(1) \ "dosage" \"doseAndRate" \ "doseQuantity" \ "value").extract[Seq[Double]] shouldBe Seq(10)
      (results.apply(1) \ "dosage" \"doseAndRate" \ "doseQuantity" \ "code").extract[Seq[String]] shouldBe Seq("mg")
    }
  }

  it should "map test data and write it to FHIR repo successfully" in {
    //Send it to our fhir repo if they are also validated
    assume(fhirServerIsAvailable)
    fhirMappingJobManager
      .executeMappingJob(tasks = Seq(medicationUsedMappingTask), sinkSettings = fhirSinkSetting)
      .map( unit =>
        unit shouldBe ()
      )
  }

  "device used mapping" should "map test data" in {
    //Some semantic tests on generated content
    fhirMappingJobManager.executeMappingTaskAndReturn(task = deviceUsedMappingTask) map { results =>
      results.length shouldBe 2
      (results.last \ "subject" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Patient", "p2")
      (results.last \ "device" \"identifier" \ "value").extract[String] shouldBe "levodopa-infusion-pump"
    }
  }

  it should "map test data and write it to FHIR repo successfully" in {
    //Send it to our fhir repo if they are also validated
    assume(fhirServerIsAvailable)
    fhirMappingJobManager
      .executeMappingJob(tasks = Seq(deviceUsedMappingTask), sinkSettings = fhirSinkSetting)
      .map( unit =>
        unit shouldBe ()
      )
  }


}
