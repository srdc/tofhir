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

class Pilot3Part2IntegrationTest  extends ToFhirTestSpec {

  override val mappingRepository: IFhirMappingRepository =
    new FhirMappingFolderRepository(Paths.get("mappings/pilot3-p2").toAbsolutePath.toUri)

  override val contextLoader: IMappingContextLoader = new MappingContextLoader(mappingRepository)

  override val schemaRepository = new SchemaFolderRepository(Paths.get("schemas/pilot3-p2").toAbsolutePath.toUri)

  val dataSourceSettings: FileSystemSourceSettings = FileSystemSourceSettings("test-source-1", "http://hus.fi", Paths.get("test-data/pilot3-p2").toAbsolutePath.toString)

  val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, sparkSession, MappingErrorHandling.withName(ToFhirConfig.mappingErrorHandling))

  val fhirSinkSetting: FhirRepositorySinkSettings = FhirRepositorySinkSettings(fhirRepoUrl = "http://localhost:8081/fhir", writeErrorHandling = MappingErrorHandling.CONTINUE)
  val onFhirClient = OnFhirNetworkClient.apply(fhirSinkSetting.fhirRepoUrl)

  val fhirServerIsAvailable =
    Try(Await.result(onFhirClient.search("Patient").execute(), FiniteDuration(5, TimeUnit.SECONDS)).httpStatus == StatusCodes.OK)
      .getOrElse(false)

  val patientMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot3-p2/patient-mapping",
    sourceContext = Map("source" ->  FileSystemSource(path = "patients.csv", sourceType = SourceFileFormats.CSV, dataSourceSettings))
  )

  val conditionMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot3-p2/condition-mapping",
    sourceContext = Map("source" ->  FileSystemSource(path = "conditions.csv", sourceType = SourceFileFormats.CSV, dataSourceSettings))
  )

  val labResultsMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot3-p2/lab-results-mapping",
    sourceContext = Map("source" ->  FileSystemSource(path = "lab-results.csv", sourceType = SourceFileFormats.CSV, dataSourceSettings))
  )

  val symptomMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot3-p2/symptom-observation-mapping",
    sourceContext = Map("source" ->  FileSystemSource(path = "symptoms.csv", sourceType = SourceFileFormats.CSV, dataSourceSettings))
  )

  val vitalSignsMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot3-p2/vital-signs-mapping",
    sourceContext = Map("source" ->  FileSystemSource(path = "vitalsigns.csv", sourceType = SourceFileFormats.CSV, dataSourceSettings))
  )

  val neuroObsMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot3-p2/neurooncological-observation-mapping",
    sourceContext = Map("source" ->  FileSystemSource(path = "neurooncological-observations.csv", sourceType = SourceFileFormats.CSV, dataSourceSettings))
  )

  val medAdministrationMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot3-p2/medication-administration-mapping",
    sourceContext = Map("source" ->  FileSystemSource(path = "medication-administrations.csv", sourceType = SourceFileFormats.CSV, dataSourceSettings))
  )

  val medUsedMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot3-p2/medication-used-mapping",
    sourceContext = Map("source" ->  FileSystemSource(path = "medications-used.csv", sourceType = SourceFileFormats.CSV, dataSourceSettings))
  )

  "patient mapping" should "map test data" in {
    //Some semantic tests on generated content
    fhirMappingJobManager.executeMappingTaskAndReturn(task = patientMappingTask) map { results =>
      results.length shouldBe 10
      (JArray(results.toList) \ "meta" \ "profile").extract[Seq[Seq[String]]].flatten.toSet shouldBe Set("https://aiccelerate.eu/fhir/StructureDefinition/AIC-Patient")
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

  "condition mapping" should "map test data" in {
    //Some semantic tests on generated content
    fhirMappingJobManager.executeMappingTaskAndReturn(task = conditionMappingTask) map { results =>
      results.length shouldBe 5

      (results.apply(1) \ "subject" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Patient", "p2")

      (results.apply(3) \ "code" \ "coding" \ "code").extract[Seq[String]].head shouldBe "M89.9"
      (results.apply(3) \ "code" \ "coding" \ "display").extract[Seq[String]].head shouldBe "Disorder of bone, unspecified"

      (results.head \ "clinicalStatus" \ "coding" \ "code").extract[Seq[String]].head shouldBe "resolved"
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

  "lab results mapping" should "map test data" in {
    //Some semantic tests on generated content
    fhirMappingJobManager.executeMappingTaskAndReturn(task = labResultsMappingTask) map { results =>
      results.length shouldBe 33
      (results.apply(25) \ "subject" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Patient", "p21")
      (results.apply(25) \  "code" \ "coding" \ "code").extract[Seq[String]].head shouldBe "2141-0"
      (results.apply(25) \  "code" \ "coding" \ "display").extract[Seq[String]].head shouldBe "Corticotropin [Mass/volume] in Plasma (P-ACTH)"
      (results.apply(25) \  "valueQuantity" \ "value").extract[Double] shouldBe 18.16060583
      (results.apply(25) \  "valueQuantity" \ "unit").extract[String] shouldBe "pg/mL"

      (results.apply(24) \  "valueQuantity" \ "value").extract[Double] shouldBe 123.06613514285
      (results.apply(24) \  "valueQuantity" \ "unit").extract[String] shouldBe "ng/mL"
    }
  }

  it should "map test data and write it to FHIR repo successfully" in {
    //Send it to our fhir repo if they are also validated
    assume(fhirServerIsAvailable)
    fhirMappingJobManager
      .executeMappingJob(tasks = Seq(labResultsMappingTask), sinkSettings = fhirSinkSetting)
      .map( unit =>
        unit shouldBe ()
      )
  }

  "symptoms mapping" should "map test data" in {
    //Some semantic tests on generated content
    fhirMappingJobManager.executeMappingTaskAndReturn(task = symptomMappingTask) map { results =>
      results.length shouldBe 12

      val patient1 = results.filter(r => (r\ "subject" \ "reference").extract[String] ==  FhirMappingUtility.getHashedReference("Patient", "p1"))
      patient1.length shouldBe 7
      (JArray(patient1.toList) \ "valueBoolean").extract[Seq[Boolean]] shouldBe (1 to 7).map(_ => false)
      (patient1.apply(4) \  "code" \ "coding" \ "code").extract[Seq[String]].head shouldBe "699281009"
      (patient1.apply(4) \  "code" \ "coding" \ "display").extract[Seq[String]].head shouldBe "Motor Weakness"

      val patient2 = results.filter(r => (r\ "subject" \ "reference").extract[String] ==  FhirMappingUtility.getHashedReference("Patient", "p2"))
      (JArray(patient2.toList) \ "valueBoolean").extract[Seq[Boolean]] shouldBe (8 to 12).map(_ => true)
    }
  }

  it should "map test data and write it to FHIR repo successfully" in {
    //Send it to our fhir repo if they are also validated
    assume(fhirServerIsAvailable)
    fhirMappingJobManager
      .executeMappingJob(tasks = Seq(symptomMappingTask), sinkSettings = fhirSinkSetting)
      .map( unit =>
        unit shouldBe ()
      )
  }

  "vital signs mapping" should "map test data" in {
    //Some semantic tests on generated content
    fhirMappingJobManager.executeMappingTaskAndReturn(task = vitalSignsMappingTask) map { results =>
      results.length shouldBe 8
    }
  }

  it should "map test data and write it to FHIR repo successfully" in {
    //Send it to our fhir repo if they are also validated
    assume(fhirServerIsAvailable)
    fhirMappingJobManager
      .executeMappingJob(tasks = Seq(vitalSignsMappingTask), sinkSettings = fhirSinkSetting)
      .map( unit =>
        unit shouldBe ()
      )
  }

  "neurooncological observations mapping" should "map test data" in {
    //Some semantic tests on generated content
    fhirMappingJobManager.executeMappingTaskAndReturn(task = neuroObsMappingTask) map { results =>
       results.length shouldBe 21
      (results.apply(9) \ "subject" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Patient", "p2")
      (results.apply(9) \  "code" \ "coding" \ "code").extract[Seq[String]].head shouldBe "18156-0"
      (results.apply(9) \  "code" \ "coding" \ "display").extract[Seq[String]].head shouldBe "Posterior wall thickness (Left ventricular posterior wall Thickness during systole by US)"
      (results.apply(9) \  "valueQuantity" \ "value").extract[Double] shouldBe 9.035436143
      (results.apply(9) \  "valueQuantity" \ "code").extract[String] shouldBe "mm"


      (results.apply(17) \  "valueCodeableConcept" \ "coding" \ "code").extract[Seq[String]].head shouldBe "LA28366-5"
      (results.apply(17) \  "valueCodeableConcept" \ "coding" \ "display").extract[Seq[String]].head shouldBe "Complete response"
    }
  }

  it should "map test data and write it to FHIR repo successfully" in {
    //Send it to our fhir repo if they are also validated
    assume(fhirServerIsAvailable)
    fhirMappingJobManager
      .executeMappingJob(tasks = Seq(neuroObsMappingTask), sinkSettings = fhirSinkSetting)
      .map( unit =>
        unit shouldBe ()
      )
  }

  "medication administration mapping" should "map test data" in {
    //Some semantic tests on generated content
    fhirMappingJobManager.executeMappingTaskAndReturn(task = medAdministrationMappingTask) map { results =>
      results.length shouldBe 12

      (results.apply(4) \ "subject" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Patient", "p3")
      (results.apply(4) \  "medicationCodeableConcept" \ "coding" \ "display").extract[Seq[String]].head shouldBe "etoposide"
    }
  }

  it should "map test data and write it to FHIR repo successfully" in {
    //Send it to our fhir repo if they are also validated
    assume(fhirServerIsAvailable)
    fhirMappingJobManager
      .executeMappingJob(tasks = Seq(medAdministrationMappingTask), sinkSettings = fhirSinkSetting)
      .map( unit =>
        unit shouldBe ()
      )
  }

  "medication used mapping" should "map test data" in {
    //Some semantic tests on generated content
    fhirMappingJobManager.executeMappingTaskAndReturn(task = medUsedMappingTask) map { results =>
      results.length shouldBe 10
      (results.apply(4) \ "subject" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Patient", "p5")
      (results.apply(4) \  "medicationCodeableConcept" \ "coding" \ "display").extract[Seq[String]].head shouldBe "vancomycin"
      (results.apply(4) \ "effectivePeriod" \ "start").extract[String] shouldBe "2014-05-10"
      (results.apply(4) \ "effectivePeriod" \ "end").extract[String] shouldBe "2014-06-10"

      (results.last \ "status").extract[String] shouldBe "active"
    }
  }

  it should "map test data and write it to FHIR repo successfully" in {
    //Send it to our fhir repo if they are also validated
    assume(fhirServerIsAvailable)
    fhirMappingJobManager
      .executeMappingJob(tasks = Seq(medUsedMappingTask), sinkSettings = fhirSinkSetting)
      .map( unit =>
        unit shouldBe ()
      )
  }




}
