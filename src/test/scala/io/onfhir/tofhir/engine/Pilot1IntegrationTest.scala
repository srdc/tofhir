package io.onfhir.tofhir.engine

import akka.http.scaladsl.model.StatusCodes
import io.onfhir.client.OnFhirNetworkClient
import io.onfhir.path.FhirPathEvaluator
import io.onfhir.tofhir.ToFhirTestSpec
import io.onfhir.tofhir.model.{FhirMappingTask, FhirRepositorySinkSettings, FileSystemSource, FileSystemSourceSettings, SourceFileFormats}
import io.onfhir.tofhir.util.FhirMappingUtility
import io.onfhir.util.JsonFormatter.formats
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.json4s.JArray

import java.net.URI
import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

class Pilot1IntegrationTest extends ToFhirTestSpec {

  val sparkConf:SparkConf = new SparkConf()
    .setAppName("tofhir-test")
    .setMaster("local[4]")
    .set("spark.driver.allowMultipleContexts", "false")
    .set("spark.ui.enabled", "false")

  val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  Paths.get(".") .toAbsolutePath

  val mappingRepository: IFhirMappingRepository =
    new FhirMappingFolderRepository(Paths.get("mappings/pilot1").toAbsolutePath.toUri)

  val contextLoader: IMappingContextLoader = new MappingContextLoader(mappingRepository)

  val schemaRepository = new SchemaFolderRepository(Paths.get("schemas/pilot1").toAbsolutePath.toUri)

  val dataSourceSettings: FileSystemSourceSettings = FileSystemSourceSettings("test-source-1", "http://hus.fi", Paths.get("test-data/pilot1").toAbsolutePath.toUri)

  val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, sparkSession)

  val fhirSinkSetting: FhirRepositorySinkSettings = FhirRepositorySinkSettings("http://localhost:8081/fhir")
  val onFhirClient = OnFhirNetworkClient.apply(fhirSinkSetting.fhirRepoUrl)

  val fhirServerIsAvailable =
    Try(Await.result(onFhirClient.search("Patient").execute(), FiniteDuration(5, TimeUnit.SECONDS)).httpStatus == StatusCodes.OK)
      .getOrElse(false)

  val patientMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot1/patient-mapping",
    sourceContext = Map("source" ->  FileSystemSource(
      path = "patients.csv",
      sourceType = SourceFileFormats.CSV,
      dataSourceSettings
    ))
  )
  val encounterMappingTask =
    FhirMappingTask(
      mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot1/operation-episode-encounter-mapping",
      sourceContext = Map("source" ->  FileSystemSource(
        path = "operation-episode-encounters.csv",
        sourceType = SourceFileFormats.CSV,
        dataSourceSettings
      ))
    )
  val surgeryPlanMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot1/surgery-plan-mapping",
    sourceContext = Map("source" ->FileSystemSource(
      path = "surgery-plans.csv",
      sourceType = SourceFileFormats.CSV,
      dataSourceSettings
    ))
  )

  val surgeryDetailsMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot1/surgery-details-mapping",
    sourceContext = Map("source" ->FileSystemSource(
      path = "surgery-details.csv",
      sourceType = SourceFileFormats.CSV,
      dataSourceSettings
    ))
  )

  "patient mapping" should "map test data" in {
    //Some semantic tests on generated content
    fhirMappingJobManager.executeMappingTaskAndReturn(task = patientMappingTask) map { results =>
      val genders = (JArray(results.toList) \ "gender").extract[Seq[String]]
      genders shouldBe ((1 to 5).map(_ => "male") ++ (6 to 10).map(_=> "female"))

      (results.apply(2) \ "id").extract[String] shouldBe FhirMappingUtility.getHashedId("Patient", "p3")
      (results.apply(2) \ "identifier" \ "value").extract[Seq[String]] shouldBe Seq("p3")
      (results.apply(2) \ "birthDate").extract[String] shouldBe("1997-02")
      (results.apply(3) \ "address" \ "postalCode").extract[Seq[String]] shouldBe(Seq("H10564"))
      (results.apply(4) \ "deceasedDateTime" ).extract[String] shouldBe("2019-04-21")
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


  "operation episode encounter mapping" should "map test data" in {
    fhirMappingJobManager.executeMappingTaskAndReturn(task = encounterMappingTask) map { results =>
      results.size shouldBe(10)
      (results.apply(1) \ "id").extract[String] shouldBe FhirMappingUtility.getHashedId("Encounter", "e2")
      (results.apply(1) \ "identifier" \ "value").extract[Seq[String]] shouldBe Seq("e2")
      (results.apply(1) \ "subject" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Patient", "p2")
      (results.apply(3) \ "episodeOfCare" \ "reference").extract[Seq[String]] shouldBe Seq( FhirMappingUtility.getHashedReference("EpisodeOfCare", "ep4"))
      (results.apply(9) \ "status").extract[String] shouldBe "cancelled"
      (results.apply(5) \ "type" \ "coding" \ "code").extract[Seq[String]] shouldBe Seq("305354007")
      (results.apply(5) \ "type" \ "coding" \ "display").extract[Seq[String]] shouldBe Seq("Ward stay")
      (results.apply(6) \ "period" \ "start").extract[String] shouldBe "2014-10-20"
      (results.apply(3) \ "period" \ "end").extractOpt[String] shouldBe empty
      (results.apply(7) \ "location" \ "location" \ "reference").extract[Seq[String]] shouldBe Seq(FhirMappingUtility.getHashedReference("Location", "ward2"))
      (results.head \ "participant" \ "individual" \ "reference").extract[Seq[String]] shouldBe Seq(FhirMappingUtility.getHashedReference("PractitionerRole", "pr4"))
    }
  }
  it should "map test data and write it to FHIR repo successfully" in {
    assert(fhirServerIsAvailable)
     fhirMappingJobManager
       .executeMappingJob(tasks = Seq(encounterMappingTask), sinkSettings = fhirSinkSetting)
       .map( unit =>
         unit shouldBe ()
       )
  }

  "surgery plan mapping" should "map test data" in {
    fhirMappingJobManager.executeMappingTaskAndReturn(task = surgeryPlanMappingTask) map { results =>
      results.size shouldBe 4
      //ServiceRequests
      val sr1 = results.find(r => (r \"id").extractOpt[String].contains(FhirMappingUtility.getHashedId("ServiceRequest", "sp1")))
      val sr2 = results.find(r => (r \"id").extractOpt[String].contains(FhirMappingUtility.getHashedId("ServiceRequest", "sp2")))
      sr1.isDefined shouldBe true
      sr2.isDefined shouldBe true
      //Encounters
      val e1 = results.find(r => (r \"episodeOfCare" \ "reference").extractOpt[Seq[String]].contains(Seq(FhirMappingUtility.getHashedReference("EpisodeOfCare","ep11"))))
      val e2 = results.find(r => (r \"episodeOfCare" \ "reference").extractOpt[Seq[String]].contains(Seq(FhirMappingUtility.getHashedReference("EpisodeOfCare","ep12"))))
      e1.isDefined shouldBe true
      e2.isDefined shouldBe true

      (sr1.get \ "subject" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Patient", "p1")
      (sr2.get \ "encounter" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Encounter","e22")
      (sr1.get \ "occurrencePeriod" \ "start").extract[String] shouldBe "2007-09-22T10:00:00+01:00"
      (sr2.get \ "occurrencePeriod" \ "end").extract[String] shouldBe "2007-09-22T16:00:00+01:00"
      (sr1.get \ "requester" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Practitioner","pr1")
      (sr1.get \ "performer" \ "reference").extractOpt[Seq[String]] shouldBe Some(Seq(FhirMappingUtility.getHashedReference("PractitionerRole","pr1")))
      (sr2.get \ "code" \ "coding" \ "code").extract[Seq[String]] shouldBe Seq("AAA27")

      (e1.get \ "subject" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Patient","p1")
      (e2.get \ "type" \ "coding" \ "code").extract[Seq[String]] shouldBe Seq("305351004")
    }

  }

  it should "map test data and write it to FHIR repo successfully" in {
   assert(fhirServerIsAvailable)
   fhirMappingJobManager
     .executeMappingJob(tasks = Seq(surgeryPlanMappingTask), sinkSettings = fhirSinkSetting)
     .map( unit =>
       unit shouldBe ()
     )
  }

  "surgery details mapping" should "map test data" in {
    fhirMappingJobManager.executeMappingTaskAndReturn(task = surgeryDetailsMappingTask) map { results =>
      results.size shouldBe 15
      val surgeryEncounters = results.filter(r => (r \ "meta" \ "profile").extract[Seq[String]].head == "https://aiccelerate.eu/fhir/StructureDefinition/AIC-SurgeryEncounter")
      surgeryEncounters.size shouldBe 2
      (surgeryEncounters.head \ "id").extract[String] shouldBe FhirMappingUtility.getHashedId("Encounter", "e11")
      (surgeryEncounters.head \ "serviceType" \ "coding" \ "code").extract[Seq[String]] shouldBe Seq("394609007")
      (surgeryEncounters.head \ "serviceType" \ "coding" \ "display").extract[Seq[String]] shouldBe Seq("General surgery")
      (surgeryEncounters.last \ "episodeOfCare" \ "reference").extract[Seq[String]] shouldBe Seq(FhirMappingUtility.getHashedReference("EpisodeOfCare", "ep2"))
      (surgeryEncounters.last \ "location" \ "location" \ "reference").extract[Seq[String]] shouldBe Seq(FhirMappingUtility.getHashedReference("Location", "or1"))
      (surgeryEncounters.head \ "basedOn" \  "reference").extract[Seq[String]] shouldBe Seq(FhirMappingUtility.getHashedReference("ServiceRequest", "sp1"))
      (surgeryEncounters.last \ "basedOn" \  "reference").extract[Seq[String]] shouldBe empty
      (surgeryEncounters.head \ "participant" \ "individual" \ "reference").extract[Seq[String]].length shouldBe 5
      //p1 has all 3 phases
      val otherPhases = results.filter(r => (r \ "meta" \ "profile").extract[Seq[String]].head == "https://aiccelerate.eu/fhir/StructureDefinition/AIC-OperationPhaseDetails")
      otherPhases.length shouldBe 3
      //Enrollment
      val enrollment = otherPhases.find(p => (p \ "category" \ "coding" \ "code").extract[Seq[String]] == Seq("305408004"))
      enrollment should not be empty
      (enrollment.head \ "performedPeriod" \ "start").extract[String] shouldBe "2015-05-17T10:05:00+01:00"
      //Both patients has anesthesia data
      val anesthesiaPhases = results.filter(r => (r \ "meta" \ "profile").extract[Seq[String]].head == "https://aiccelerate.eu/fhir/StructureDefinition/AIC-AnesthesiaPhaseDetails")
      anesthesiaPhases.length shouldBe 2
      val patient1AnesthesiaProcedure = anesthesiaPhases.find(r => (r \ "subject" \ "reference").extract[String] == FhirMappingUtility.getHashedReference("Patient", "p1"))
      patient1AnesthesiaProcedure should not be empty
      (patient1AnesthesiaProcedure.head \ "code" \ "coding" \ "code").extract[Seq[String]].toSet shouldBe Set("50697003","WX402")
      (patient1AnesthesiaProcedure.head \ "code" \ "coding" \ "display").extract[Seq[String]].toSet shouldBe Set("General Anesthesia","General anesthesis")

      val surgeryPhases = results.filter(r => (r \ "meta" \ "profile").extract[Seq[String]].head == "https://aiccelerate.eu/fhir/StructureDefinition/AIC-SurgeryPhaseDetails")
      surgeryPhases.length shouldBe 2
      val patient1Surgery = surgeryPhases.find(r => (r \ "subject" \ "reference").extract[String] == FhirMappingUtility.getHashedReference("Patient", "p1"))
      patient1Surgery should not be empty
      (patient1Surgery.head \ "code" \ "coding" \ "code").extract[Seq[String]] shouldBe Seq("AAC00")
      (patient1Surgery.head \ "code" \ "coding" \ "display").extract[Seq[String]] shouldBe Seq("Ligature of intracranial aneurysm")
      //3 surgeons for p1
      FhirPathEvaluator().evaluateString("Procedure.performer.where(function.coding.where(code='304292004').exists()).actor.reference", patient1Surgery.head ).length shouldBe 3

      val patient2Surgery = surgeryPhases.find(r => (r \ "subject" \ "reference").extract[String] == FhirMappingUtility.getHashedReference("Patient", "p2"))
      patient2Surgery should not be empty
      //For patient 2, 2 procedure from other procedures and 1 from intubation
      val otherProcedures = results.filter(r => (r \ "meta" \ "profile").extract[Seq[String]].head == "https://aiccelerate.eu/fhir/StructureDefinition/AIC-ProcedureRelatedWithSurgicalWorkflow")
      otherProcedures.length shouldBe 3
      (JArray(otherProcedures.toList) \ "code" \ "coding" \ "code").extract[Seq[String]].toSet shouldBe Set("ADA99","ADB","232678001")
      (JArray(otherProcedures.toList) \ "partOf" \ "reference").extract[Seq[String]].toSet should contain ("Procedure/"+(patient2Surgery.head \ "id").extract[String])
      //both patient has this data
      val surgicalWounds = results.filter(r => (r \ "meta" \ "profile").extract[Seq[String]].head == "https://aiccelerate.eu/fhir/StructureDefinition/AIC-SurgicalWoundClassificationObservation")
      surgicalWounds.length shouldBe 2
      //Both has classification 2
      (JArray(surgicalWounds.toList) \ "valueCodeableConcept" \ "coding" \ "code").extract[Seq[String]].toSet shouldBe Set("418115006")
      (JArray(surgicalWounds.toList) \ "valueCodeableConcept" \ "coding" \ "display").extract[Seq[String]].toSet shouldBe Set("Clean-contaminated (Class II)")
      //Only patient 2 has this data
      val punctures = results.filter(r => (r \ "meta" \ "profile").extract[Seq[String]].head == "https://aiccelerate.eu/fhir/StructureDefinition/AIC-IntraOperativeObservation")
      punctures.length shouldBe 1
      (punctures.head \ "encounter" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Encounter", "e12")
      (punctures.head \ "valueQuantity" \ "value").extract[Int] shouldBe 2
    }
  }

  it should "map test data and write it to FHIR repo successfully" in {
    assert(fhirServerIsAvailable)
    fhirMappingJobManager
      .executeMappingJob(tasks = Seq(surgeryDetailsMappingTask), sinkSettings = fhirSinkSetting)
      .map( unit =>
        unit shouldBe ()
      )
  }

}
