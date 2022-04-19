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
import org.json4s.JsonAST.JObject

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
    sourceContext = Map("source" ->  FileSystemSource(path = "patients.csv", sourceType = SourceFileFormats.CSV, dataSourceSettings))
  )
  val encounterMappingTask =
    FhirMappingTask(
      mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot1/operation-episode-encounter-mapping",
      sourceContext = Map("source" ->  FileSystemSource(path = "operation-episode-encounters.csv", sourceType = SourceFileFormats.CSV, dataSourceSettings))
    )
  val surgeryPlanMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot1/surgery-plan-mapping",
    sourceContext = Map("source" ->FileSystemSource(path = "surgery-plans.csv", sourceType = SourceFileFormats.CSV, dataSourceSettings))
  )

  val surgeryDetailsMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot1/surgery-details-mapping",
    sourceContext = Map("source" ->FileSystemSource(path = "surgery-details.csv", sourceType = SourceFileFormats.CSV, dataSourceSettings))
  )

  val preopAssMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot1/preoperative-assessment-mapping",
    sourceContext = Map("source" ->FileSystemSource(path = "preoperative-assessment.csv", sourceType = SourceFileFormats.CSV, dataSourceSettings))
  )

  val healthBehaviorMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot1/health-behavior-assessment-mapping",
    sourceContext = Map("source" ->FileSystemSource(path = "health-behavior-assessment.csv", sourceType = SourceFileFormats.CSV, dataSourceSettings
    ))
  )

  val otherObsMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot1/other-observation-mapping",
    sourceContext = Map("source" ->FileSystemSource(path = "other-observations.csv", sourceType = SourceFileFormats.CSV, dataSourceSettings))
  )

  val medUsedMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot1/medication-used-mapping",
    sourceContext = Map("source" ->FileSystemSource(path = "medication-used.csv", sourceType = SourceFileFormats.CSV, dataSourceSettings
    ))
  )

  val medAdmMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot1/medication-administration-mapping",
    sourceContext = Map("source" ->FileSystemSource(path = "medication-administration.csv", sourceType = SourceFileFormats.CSV, dataSourceSettings))
  )

  val conditionMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot1/condition-mapping",
    sourceContext = Map("source" ->FileSystemSource(path = "conditions.csv", sourceType = SourceFileFormats.CSV, dataSourceSettings))
  )

  val vitalSignsMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot1/vital-signs-mapping",
    sourceContext = Map("source" ->FileSystemSource(path = "vitalsigns.csv", sourceType = SourceFileFormats.CSV, dataSourceSettings))
  )

  val labResultsMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot1/lab-results-mapping",
    sourceContext = Map("source" ->FileSystemSource(path = "lab-results.csv", sourceType = SourceFileFormats.CSV, dataSourceSettings))
  )

  val radStudiesMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot1/radiological-studies-mapping",
    sourceContext = Map("source" ->FileSystemSource(path = "radiological-studies.csv", sourceType = SourceFileFormats.CSV, dataSourceSettings))
  )

  val hospitalUnitMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot1/hospital-unit-mapping",
    sourceContext = Map("source" ->FileSystemSource(path = "hospital-unit.csv", sourceType = SourceFileFormats.CSV, dataSourceSettings))
  )

  val practitionerMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot1/practitioner-mapping",
    sourceContext = Map("source" ->FileSystemSource(path = "practitioners.csv", sourceType = SourceFileFormats.CSV, dataSourceSettings))
  )

  val workshiftMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot1/workshift-mapping",
    sourceContext = Map("source" ->FileSystemSource(path = "workshift.csv", sourceType = SourceFileFormats.CSV, dataSourceSettings))
  )

  val backgroundInfMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot1/background-information-mapping",
    sourceContext = Map("source" ->FileSystemSource(path = "background-information.csv", sourceType = SourceFileFormats.CSV, dataSourceSettings))
  )

  val preopRisksMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot1/preoperative-risks-mapping",
    sourceContext = Map("source" ->FileSystemSource(path = "preoperative-risk-factors.csv", sourceType = SourceFileFormats.CSV, dataSourceSettings))
  )

  val patientReportedConditionsMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot1/patient-reported-conditions-mapping",
    sourceContext = Map("source" ->FileSystemSource(path = "patient-reported-conditions.csv", sourceType = SourceFileFormats.CSV, dataSourceSettings))
  )

  val preopSymptomsMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot1/preoperative-symptoms-mapping",
    sourceContext = Map("source" ->FileSystemSource(path = "preoperative-symptoms.csv", sourceType = SourceFileFormats.CSV, dataSourceSettings))
  )

  val anestObsMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot1/anesthesia-observations-mapping",
    sourceContext = Map("source" ->FileSystemSource(path = "anesthesia-observations.csv", sourceType = SourceFileFormats.CSV, dataSourceSettings))
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

  "preoperative assessment mapping" should "map test data" in {
    fhirMappingJobManager.executeMappingTaskAndReturn(task = preopAssMappingTask) map { results =>
      results should not be empty
      val asaObs = results.filter(r => (r \ "meta" \ "profile").extract[Seq[String]].head == "https://aiccelerate.eu/fhir/StructureDefinition/AIC-ASAClassification")
      asaObs.length shouldBe 10
      (JArray(asaObs.toList) \ "subject" \ "reference").extract[Seq[String]].toSet shouldBe (1 to 10).map(i => s"p$i").map(pid => FhirMappingUtility.getHashedReference("Patient", pid)).toSet
      (JArray(asaObs.toList) \ "encounter" \ "reference").extract[Seq[String]].toSet shouldBe (1 to 10).map(i => s"e$i").map(pid => FhirMappingUtility.getHashedReference("Encounter", pid)).toSet

      val patient3Asa  = asaObs.find(r => (r \ "subject" \ "reference").extract[String] == FhirMappingUtility.getHashedReference("Patient", "p3"))
      (patient3Asa.head \ "valueCodeableConcept" \ "coding" \ "code").extract[Seq[String]].head shouldBe "413499007"
      (patient3Asa.head \ "valueCodeableConcept" \ "coding" \ "display").extract[Seq[String]].head shouldBe "ASA5: A moribund patient who is not expected to survive without the operation"
      (patient3Asa.head \ "effectiveDateTime").extract[String] shouldBe "2011-01-07"

      val urgObs = results.filter(r => (r \ "meta" \ "profile").extract[Seq[String]].head == "https://aiccelerate.eu/fhir/StructureDefinition/AIC-OperationUrgency")
      urgObs.length shouldBe 10
      (JArray(urgObs.toList) \ "subject" \ "reference").extract[Seq[String]].toSet shouldBe (1 to 10).map(i => s"p$i").map(pid => FhirMappingUtility.getHashedReference("Patient", pid)).toSet
      (JArray(urgObs.toList) \ "encounter" \ "reference").extract[Seq[String]].toSet shouldBe (1 to 10).map(i => s"e$i").map(pid => FhirMappingUtility.getHashedReference("Encounter", pid)).toSet
      val patient7Urg  = urgObs.find(r => (r \ "subject" \ "reference").extract[String] == FhirMappingUtility.getHashedReference("Patient", "p7"))
      (patient7Urg.head  \ "valueCodeableConcept" \ "coding" \ "code").extract[Seq[String]].head shouldBe "R3"
      (patient7Urg.head  \ "valueCodeableConcept" \ "coding" \ "display").extract[Seq[String]].head shouldBe "Elective patient: (in 6 months)"

       val pregnancyObs = results.filter(r => (r \ "meta" \ "profile").extract[Seq[String]].head == "https://aiccelerate.eu/fhir/StructureDefinition/AIC-PregnancyStatus")
       pregnancyObs.length shouldBe 5
      (JArray(pregnancyObs.toList) \ "subject" \ "reference").extract[Seq[String]].toSet shouldBe (6 to 10).map(i => s"p$i").map(pid => FhirMappingUtility.getHashedReference("Patient", pid)).toSet
      val pregnantPatients = pregnancyObs.filter(r => (r \ "valueCodeableConcept" \ "coding" \ "code").extract[Seq[String]].head == "77386006")
      pregnantPatients.length shouldBe 1
      (pregnantPatients.head \ "subject" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Patient", "p9")

      val operationFlags = results.filter(r => (r \ "meta" \ "profile").extract[Seq[String]].head == "https://aiccelerate.eu/fhir/StructureDefinition/AIC-OperationFlag")
      operationFlags.length shouldBe 3
      (JArray(operationFlags.toList) \ "code" \ "coding" \ "code").extract[Seq[String]].toSet shouldBe Set("40174006", "day-case-surgery", "169741004")
      //Post menstrual age
      val pmage = results.filter(r => (r \ "meta" \ "profile").extract[Seq[String]].head == "https://aiccelerate.eu/fhir/StructureDefinition/AIC-PostMenstrualAge")
      pmage.length shouldBe 7
      (JArray(pmage.toList) \ "valueQuantity" \ "value").extract[Seq[Int]].toSet shouldBe Set(24, 27, 19, 35, 19, 26, 21, 29)
      //Post gestational age
      val pgage = results.filter(r => (r \ "meta" \ "profile").extract[Seq[String]].head == "https://aiccelerate.eu/fhir/StructureDefinition/AIC-PostGestationalAge")
      pgage.length shouldBe 6

      val bloodGroupObs = results.filter(r => (r \ "meta" \ "profile").extract[Seq[String]].head =="https://aiccelerate.eu/fhir/StructureDefinition/AIC-BloodGroupObservation")
      bloodGroupObs.length shouldBe 8
      val patient5BloodGroup = bloodGroupObs.find(r => (r \ "subject" \ "reference").extract[String]  == FhirMappingUtility.getHashedReference("Patient", "p5"))
      patient5BloodGroup should not be empty
      (patient5BloodGroup.head \ "valueCodeableConcept" \ "coding" \ "code").extract[Seq[String]].head shouldBe "278151004"
      (patient5BloodGroup.head \ "valueCodeableConcept" \ "coding" \ "display").extract[Seq[String]].head shouldBe "AB Rh(D) positive"
    }
  }
  it should "map test data and write it to FHIR repo successfully" in {
    assert(fhirServerIsAvailable)
    fhirMappingJobManager
      .executeMappingJob(tasks = Seq(preopAssMappingTask), sinkSettings = fhirSinkSetting)
      .map( unit =>
        unit shouldBe ()
      )
  }

  "health behaviour assessment mapping" should "map test data" in {
    fhirMappingJobManager.executeMappingTaskAndReturn(task = healthBehaviorMappingTask) map { results =>
      results.length shouldBe 9
      (results.apply(1) \ "subject" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Patient", "p2")
      //patient 3 has smoking status info
      (results.apply(2) \ "code" \ "coding" \ "code").extract[Seq[String]].head shouldBe "72166-2"

      (results.apply(2) \ "valueCodeableConcept" \ "coding" \ "code").extract[Seq[String]].head shouldBe "266919005"
      (results.apply(1) \ "valueCodeableConcept" \ "coding" \ "display").extract[Seq[String]].head shouldBe "Former smoker"

      (results.apply(4) \ "valueCodeableConcept" \ "coding" \ "display").extract[Seq[String]].head shouldBe "Current some day user"
      (results.apply(2) \ "encounter" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Encounter", "e1")
      (results.apply(8) \ "effectiveDateTime").extract[String] shouldBe "2000-10-09"
    }
  }

  it should "map test data and write it to FHIR repo successfully" in {
    assert(fhirServerIsAvailable)
    fhirMappingJobManager
      .executeMappingJob(tasks = Seq(healthBehaviorMappingTask), sinkSettings = fhirSinkSetting)
      .map( unit =>
        unit shouldBe ()
      )
  }

  "other observation mapping" should "map test data" in {
    fhirMappingJobManager.executeMappingTaskAndReturn(task = otherObsMappingTask) map { results =>
      results.length shouldBe 14
      (results.apply(1) \ "subject" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Patient", "p2")
      (results.apply(2) \ "encounter" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Encounter", "e3")

      (results.apply(4) \ "valueQuantity" \ "value").extract[Double] shouldBe 43.2
      (results.apply(4) \ "meta" \ "profile").extract[Seq[String]].head shouldBe "https://aiccelerate.eu/fhir/StructureDefinition/AIC-IntraOperativeObservation"
      (results.apply(4) \ "valueQuantity" \ "unit").extract[String] shouldBe "mL"
      (results.apply(4) \ "code" \ "coding" \ "code").extract[Seq[String]].head shouldBe "1298-9"
      (results.apply(4) \ "code" \ "coding" \ "display").extract[Seq[String]].head shouldBe "RBC given"

      (results.apply(10) \ "meta" \ "profile").extract[Seq[String]].head shouldBe "https://aiccelerate.eu/fhir/StructureDefinition/AIC-MedicationAdministration"
      (results.apply(8) \ "meta" \ "profile").extract[Seq[String]].head shouldBe "https://aiccelerate.eu/fhir/StructureDefinition/AIC-PEWSScore"

      (results.apply(13) \ "component" \ "valueQuantity" \ "value").extract[Seq[Int]] shouldBe Seq(3, 5, 4)
      (results.apply(13) \ "valueQuantity" \ "value").extract[Int] shouldBe 4
    }
  }
  it should "map test data and write it to FHIR repo successfully" in {
    assert(fhirServerIsAvailable)
    fhirMappingJobManager
      .executeMappingJob(tasks = Seq(otherObsMappingTask), sinkSettings = fhirSinkSetting)
      .map( unit =>
        unit shouldBe ()
      )
  }

  "medication used mapping" should "map test data" in {
    fhirMappingJobManager.executeMappingTaskAndReturn(task = medUsedMappingTask) map { results =>
      results.length shouldBe 5

      (results.apply(1) \ "subject" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Patient", "p2")
      (results.head \ "context" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Encounter", "e1")

      (results.apply(2) \ "medicationCodeableConcept" \ "coding" \ "code").extract[Seq[String]].head shouldBe "J01DD02"
      (results.apply(2) \ "medicationCodeableConcept" \ "coding" \ "display").extract[Seq[String]].head shouldBe "medication2"
      (results.apply(2) \ "dosage" \ "timing" \ "repeat" \ "frequency").extract[Seq[Int]].head shouldBe 1
      (results.apply(2) \ "dosage" \ "doseAndRate" \ "doseQuantity" \ "value").extract[Seq[Double]].head shouldBe 20.0
      (results.apply(2) \ "dosage" \ "doseAndRate" \ "doseQuantity" \ "unit").extract[Seq[String]].head shouldBe "mg"
    }
  }

  it should "map test data and write it to FHIR repo successfully" in {
    assert(fhirServerIsAvailable)
    fhirMappingJobManager
      .executeMappingJob(tasks = Seq(medUsedMappingTask), sinkSettings = fhirSinkSetting)
      .map( unit =>
        unit shouldBe ()
      )
  }

  "medication administration mapping" should "map test data" in {
    fhirMappingJobManager.executeMappingTaskAndReturn(task = medAdmMappingTask) map { results =>
      results.length shouldBe 5
      (results.apply(2) \ "subject" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Patient", "p3")
      (results.apply(3)  \ "context" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Encounter", "e3")

      (results.apply(2) \ "medicationCodeableConcept" \ "coding" \ "code").extract[Seq[String]].head shouldBe "L01CA02"
      (results.apply(2) \ "medicationCodeableConcept" \ "coding" \ "display").extract[Seq[String]].head shouldBe "medication2"
      (results.apply(1) \ "medicationCodeableConcept" \ "coding" \ "display").extract[Seq[String]] shouldBe empty
      (results.apply(1) \ "effectivePeriod" \ "start").extract[String] shouldBe "2005-05-29T10:20:00+01:00"
      (results.apply(4) \ "dosage" \ "dose" \ "value").extract[Double] shouldBe 40.5
      (results.apply(4) \ "dosage" \ "dose" \ "code").extract[String] shouldBe "mg"
    }
  }

  it should "map test data and write it to FHIR repo successfully" in {
    assert(fhirServerIsAvailable)
    fhirMappingJobManager
      .executeMappingJob(tasks = Seq(medAdmMappingTask), sinkSettings = fhirSinkSetting)
      .map( unit =>
        unit shouldBe ()
      )
  }

  "conditions mapping" should "map test data" in {
    fhirMappingJobManager.executeMappingTaskAndReturn(task = conditionMappingTask) map { results =>
      results.length shouldBe 5
      (results.apply(2) \ "subject" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Patient", "p3")
      (results.apply(1)  \ "encounter" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Encounter", "e2")
      (results.apply(4) \ "code" \ "coding" \ "code").extract[Seq[String]].head shouldBe "G40.419"
      (results.apply(4) \ "code" \ "coding" \ "display").extract[Seq[String]].head shouldBe "Other generalized epilepsy and epileptic syndromes, intractable, without status epilepticus"
      (results.apply(1) \ "onsetDateTime" ).extract[String] shouldBe "2013-05-07"
      (results.head \ "category" \ "coding" \ "code").extract[Seq[String]].head shouldBe "problem-list-item"
      (results.apply(2) \ "verificationStatus" \ "coding" \ "code").extract[Seq[String]].head shouldBe "unconfirmed"

      (results.apply(4) \ "asserter" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Practitioner", "pr2")
    }
  }

  it should "map test data and write it to FHIR repo successfully" in {
    assert(fhirServerIsAvailable)
    fhirMappingJobManager
      .executeMappingJob(tasks = Seq(conditionMappingTask), sinkSettings = fhirSinkSetting)
      .map( unit =>
        unit shouldBe ()
      )
  }

  "vital signs mapping" should "map test data" in {
    fhirMappingJobManager.executeMappingTaskAndReturn(task = vitalSignsMappingTask) map { results =>
      results.length shouldBe 8
      (results.apply(5) \ "subject" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Patient", "p2")
      (results.head  \ "encounter" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Encounter", "e1")
      (results.apply(4) \ "code" \ "coding" \ "code").extract[Seq[String]].head shouldBe "8867-4"
      (results.apply(4) \ "code" \ "coding" \ "display").extract[Seq[String]].head shouldBe "Heart rate"

      (results.apply(2) \ "valueQuantity" \ "value").extract[Double] shouldBe 25.5
      (results.apply(2) \ "valueQuantity" \ "code").extract[String] shouldBe "kg/m2"

      (results.apply(5) \ "component" \ "valueQuantity" \ "value").extract[Seq[Double]] shouldBe Seq(132,95)
    }
  }

  it should "map test data and write it to FHIR repo successfully" in {
    assert(fhirServerIsAvailable)
    fhirMappingJobManager
      .executeMappingJob(tasks = Seq(vitalSignsMappingTask), sinkSettings = fhirSinkSetting)
      .map( unit =>
        unit shouldBe ()
      )
  }

  "lab results mapping" should "map test data" in {
    fhirMappingJobManager.executeMappingTaskAndReturn(task = labResultsMappingTask) map { results =>
      results.length shouldBe 26
      (results.apply(1) \ "subject" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Patient", "p2")
      (results.apply(1) \ "encounter" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Encounter", "e2")
      (results.head \ "effectiveDateTime").extract[String] shouldBe "2012-05-10T10:00:00+01:00"

      (results.head \ "code" \ "coding" \"code").extract[Seq[String]].toSet shouldBe Set("1552", "718-7")
      (results.head \ "valueQuantity" \ "value").extract[Double] shouldBe 1.05
      (results.head \ "valueQuantity" \ "unit").extract[String] shouldBe "g/dL"

      (results.apply(6) \ "valueQuantity" \ "value").extract[Double] shouldBe 15 * 7.500617
      (results.apply(6) \ "valueQuantity" \ "unit").extract[String] shouldBe  "mm[Hg]"

      (results.apply(3) \ "interpretation" \ "coding" \ "code").extract[Seq[String]].head shouldBe "N"

      (results.last \ "code" \ "coding" \"code").extract[Seq[String]] shouldBe Seq("99999")
      (results.last \ "code" \ "coding" \"display").extract[Seq[String]] shouldBe Seq("Blabla")
    }
  }


  it should "map test data and write it to FHIR repo successfully" in {
    assert(fhirServerIsAvailable)
    fhirMappingJobManager
      .executeMappingJob(tasks = Seq(labResultsMappingTask), sinkSettings = fhirSinkSetting)
      .map( unit =>
        unit shouldBe ()
      )
  }

  "radiological studies mapping" should "map test data" in {
    fhirMappingJobManager.executeMappingTaskAndReturn(task = radStudiesMappingTask) map { results =>
      results.length shouldBe 5
      (results.head \ "subject" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Patient", "p1")
      (results.head \ "encounter" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Encounter", "e1")
      (results.apply(1) \ "effectiveDateTime").extract[String] shouldBe "2012-05-10"
      (results.apply(1) \ "code" \ "coding" \ "code").extract[Seq[String]].head shouldBe "PA2AT"
      (results.apply(1) \ "code" \ "coding" \ "display").extract[Seq[String]].head shouldBe "Cerebral artery PTA"
    }
  }

  it should "map test data and write it to FHIR repo successfully" in {
    assert(fhirServerIsAvailable)
    fhirMappingJobManager
      .executeMappingJob(tasks = Seq(radStudiesMappingTask), sinkSettings = fhirSinkSetting)
      .map( unit =>
        unit shouldBe ()
      )
  }

  "hospital unit mapping" should "map test data" in {
    fhirMappingJobManager.executeMappingTaskAndReturn(task = hospitalUnitMappingTask) map { results =>
      results.length shouldBe 4
      (results.apply(1) \ "type" \ "coding" \ "code").extract[Seq[String]] shouldBe Seq("309904001")
      (results.apply(1) \ "physicalType" \ "coding" \ "code").extract[Seq[String]] shouldBe Seq("ro")
      (results.apply(2) \ "id").extract[String] shouldBe FhirMappingUtility.getHashedId("Location", "ward1")

      (results.last \ "partOf" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Location", "ward1")
    }
  }

  it should "map test data and write it to FHIR repo successfully" in {
    assert(fhirServerIsAvailable)
    fhirMappingJobManager
      .executeMappingJob(tasks = Seq(hospitalUnitMappingTask), sinkSettings = fhirSinkSetting)
      .map( unit =>
        unit shouldBe ()
      )
  }

  "practitioner mapping" should "map test data" in {
    fhirMappingJobManager.executeMappingTaskAndReturn(task = practitionerMappingTask) map { results =>
      results.length shouldBe 8

      val practitioners = results.filter(r => (r \ "meta" \ "profile").extract[Seq[String]].head == "https://aiccelerate.eu/fhir/StructureDefinition/AIC-Practitioner")
      practitioners.length shouldBe 4
      (practitioners.head \ "name" \ "family").extract[Seq[String]] shouldBe Seq("NAMLI")
      (practitioners.last \ "qualification" \ "code" \ "coding" \ "code").extract[Seq[String]].head shouldBe "RN"

      val practitionerRoles = results.filter(r => (r \ "meta" \ "profile").extract[Seq[String]].head == "https://aiccelerate.eu/fhir/StructureDefinition/AIC-PractitionerRoleForSurgicalWorkflow")
      (practitionerRoles.head \ "code" \ "coding" \ "display").extract[Seq[String]].head shouldBe "Specialized surgeon"
      (practitionerRoles.head \ "specialty" \ "coding" \ "display").extract[Seq[String]].head shouldBe "Cardiology"
    }
  }

  it should "map test data and write it to FHIR repo successfully" in {
    assert(fhirServerIsAvailable)
    fhirMappingJobManager
      .executeMappingJob(tasks = Seq(practitionerMappingTask), sinkSettings = fhirSinkSetting)
      .map( unit =>
        unit shouldBe ()
      )
  }

  "workshift mapping" should "map test data" in {
    fhirMappingJobManager.executeMappingTaskAndReturn(task = workshiftMappingTask) map { results =>
      results.length shouldBe 2
      (results.head \ "actor" \"reference").extract[Seq[String]].toSet shouldBe Set(FhirMappingUtility.getHashedReference("PractitionerRole", "pr1"), FhirMappingUtility.getHashedReference("Location", "loc1"))
      (results.head \ "extension" \ "valueBoolean").extract[Seq[Boolean]].head shouldBe false
      (results.head \ "planningHorizon" \ "start").extract[String] shouldBe "2017-01-18"
      (results.head \ "planningHorizon" \ "end").extract[String] shouldBe "2017-01-19"
    }
  }

  it should "map test data and write it to FHIR repo successfully" in {
    assert(fhirServerIsAvailable)
    fhirMappingJobManager
      .executeMappingJob(tasks = Seq(workshiftMappingTask), sinkSettings = fhirSinkSetting)
      .map( unit =>
        unit shouldBe ()
      )
  }

  "background information mapping" should "map test data" in {
    fhirMappingJobManager.executeMappingTaskAndReturn(task = backgroundInfMappingTask) map { results =>
      results.length shouldBe 30
      val patient1Flags = results.filter(r => (r\ "subject" \ "reference").extract[String] == FhirMappingUtility.getHashedReference("Patient", "p1"))
      (JArray(patient1Flags.toList) \ "period" \"start").extract[Seq[String]].toSet shouldBe Set("2021-10-05T10:00:00+01:00")

      val flag8 = patient1Flags.find(r => (r \ "code" \ "coding" \ "code").extract[Seq[String]].contains("366248005"))
      flag8 should not be empty
      (flag8.head \ "code" \ "coding" \ "display").extract[Seq[String]].head shouldBe "Pivot tooth, partial denture"
      (flag8.head \ "code" \ "coding" \ "system").extract[Seq[String]].head shouldBe "http://snomed.info/sct"
    }
  }

  it should "map test data and write it to FHIR repo successfully" in {
    assert(fhirServerIsAvailable)
    fhirMappingJobManager
      .executeMappingJob(tasks = Seq(backgroundInfMappingTask), sinkSettings = fhirSinkSetting)
      .map( unit =>
        unit shouldBe ()
      )
  }

  "preoperative risk factors mapping" should "map test data" in {
    fhirMappingJobManager.executeMappingTaskAndReturn(task = preopRisksMappingTask) map { results =>
       results.length shouldBe 3
      (results.apply(1) \ "subject" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Patient", "p2")
      (results.apply(1) \ "encounter" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Encounter", "e2")
      (results.head \ "effectivePeriod" \ "start").extract[String] shouldBe "2017-10-05T10:00:00+01:00"
      (results.apply(1) \ "code" \ "coding" \ "code").extract[Seq[String]].head shouldBe "62014003+366667001"
      (results.apply(1) \ "code" \ "coding" \ "display").extract[Seq[String]].head shouldBe "Adverse reaction caused by drug (disorder) + Skin Reaction (finding)"
      val comps = (results.apply(1) \ "component").extract[Seq[JObject]]
      comps.length shouldBe 3
      (comps.head \ "code" \ "coding" \ "code").extract[Seq[String]].head shouldBe "246112005"
      (comps.head \ "valueBoolean").extract[Boolean] shouldBe true
    }
  }

  it should "map test data and write it to FHIR repo successfully" in {
    assert(fhirServerIsAvailable)
    fhirMappingJobManager
      .executeMappingJob(tasks = Seq(preopRisksMappingTask), sinkSettings = fhirSinkSetting)
      .map( unit =>
        unit shouldBe ()
      )
  }

  "patient reported conditions mapping" should "map test data" in {
    fhirMappingJobManager.executeMappingTaskAndReturn(task = patientReportedConditionsMappingTask) map { results =>
      results.length shouldBe 20
      val patient1Conds = results.filter(r => (r \ "subject" \ "reference").extract[String] == FhirMappingUtility.getHashedReference("Patient", "p1"))
      val circDisease = patient1Conds.find(r => (r \ "code" \"coding" \ "code").extract[Seq[String]].head == "400047006")
      circDisease should not be empty
      (circDisease.head \ "code" \"coding" \ "display").extract[Seq[String]].head shouldBe "circulation diseases in lower extremities"
    }
  }

  it should "map test data and write it to FHIR repo successfully" in {
    assert(fhirServerIsAvailable)
    fhirMappingJobManager
      .executeMappingJob(tasks = Seq(patientReportedConditionsMappingTask), sinkSettings = fhirSinkSetting)
      .map( unit =>
        unit shouldBe ()
      )
  }

  "preoperative symptoms mapping" should "map test data" in {
    fhirMappingJobManager.executeMappingTaskAndReturn(task = preopSymptomsMappingTask) map { results =>
      results.length shouldBe 38
      val patient1Symptoms = results.filter(r => (r \ "subject" \ "reference").extract[String] == FhirMappingUtility.getHashedReference("Patient", "p1"))
      patient1Symptoms.length shouldBe 19
      (patient1Symptoms.head  \ "encounter" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Encounter", "e1")

      (patient1Symptoms.apply(11) \ "code" \ "coding" \ "code").extract[Seq[String]].head shouldBe "267036007:371881003=870595007"
      (patient1Symptoms.apply(11) \ "code" \ "coding" \ "display").extract[Seq[String]].head shouldBe "Dyspnea during walking"
    }
  }

  it should "map test data and write it to FHIR repo successfully" in {
    assert(fhirServerIsAvailable)
    fhirMappingJobManager
      .executeMappingJob(tasks = Seq(preopSymptomsMappingTask), sinkSettings = fhirSinkSetting)
      .map( unit =>
        unit shouldBe ()
      )
  }

  "anesthesia observations mapping" should "map test data" in {
    fhirMappingJobManager.executeMappingTaskAndReturn(task = anestObsMappingTask) map { results =>
      results.length shouldBe 130
      val p1 = results.filter(r => (r \ "encounter" \ "reference").extractOpt[String].contains(FhirMappingUtility.getHashedReference("Encounter", "e1")))
      p1.length shouldBe 65
      (p1.apply(35) \ "effectiveDateTime").extract[String] shouldBe "2015-05-10T10:25:00+01:00"

      (p1.apply(42) \ "code" \ "coding" \ "code").extract[Seq[String]].head shouldBe "61010-5"
      (p1.apply(42) \ "code" \ "coding" \ "display").extract[Seq[String]].head shouldBe "Burst suppression ratio [Ratio] Cerebral cortex Electroencephalogram (EEG)"
      (p1.apply(42) \ "valueQuantity" \ "value").extract[Double] shouldBe 0.277386451
      (p1.apply(42) \ "valueQuantity" \ "unit").extract[String] shouldBe "%"
    }
  }

  it should "map test data and write it to FHIR repo successfully" in {
    assert(fhirServerIsAvailable)
    fhirMappingJobManager
      .executeMappingJob(tasks = Seq(anestObsMappingTask), sinkSettings = fhirSinkSetting)
      .map( unit =>
        unit shouldBe ()
      )
  }
}
