package io.onfhir.tofhir.engine

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import com.typesafe.scalalogging.Logger
import io.onfhir.api.client.FhirBatchTransactionRequestBuilder
import io.onfhir.api.util.FHIRUtil
import io.onfhir.client.OnFhirNetworkClient
import io.onfhir.tofhir.ToFhirTestSpec
import io.onfhir.tofhir.config.MappingErrorHandling
import io.onfhir.tofhir.model._
import io.onfhir.tofhir.util.FhirMappingUtility
import io.onfhir.util.JsonFormatter.formats
import org.scalatest.Assertion

import java.io.File
import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, Future}
import scala.util.Try

class FhirMappingJobManagerTest extends ToFhirTestSpec {

  private val logger: Logger = Logger(this.getClass)

  private def deleteResources(): Future[Assertion] = {
    // Start delete operation of written resources on the FHIR
    var batchRequest: FhirBatchTransactionRequestBuilder = onFhirClient.batch()
    // Delete all patients between p1-p10 and related observations
    val obsSearchFutures = (1 to 10).map(i => {
      batchRequest = batchRequest.entry(_.delete("Patient", FhirMappingUtility.getHashedId("Patient", "p"+i)))
      onFhirClient.search("Observation").where("subject", "Patient/" + FhirMappingUtility.getHashedId("Patient", "p"+i))
        .executeAndReturnBundle()
    })
    Future.sequence(obsSearchFutures) flatMap { obsBundleList =>
      obsBundleList.foreach(observationBundle => {
        observationBundle.searchResults.foreach(obs =>
          batchRequest = batchRequest.entry(_.delete("Observation", (obs \ "id").extract[String]))
        )
      })
      // delete MedicationAdministration records by patient
      val medicationAdminSearchFutures = List(1,2,4).map(i => {
        batchRequest = batchRequest.entry(_.delete("Patient", FhirMappingUtility.getHashedId("Patient", "p"+i)))
        onFhirClient.search("MedicationAdministration").where("subject", "Patient/" + FhirMappingUtility.getHashedId("Patient", "p"+i))
          .executeAndReturnBundle()
      })
      Future.sequence(medicationAdminSearchFutures) flatMap { medicationAdminBundleList =>
        medicationAdminBundleList.foreach(medicationAdminBundle => {
          medicationAdminBundle.searchResults.foreach(mAdmin =>
            batchRequest = batchRequest.entry(_.delete("MedicationAdministration", (mAdmin \ "id").extract[String]))
          )
        })
        batchRequest.returnMinimal().asInstanceOf[FhirBatchTransactionRequestBuilder].execute() map { res =>
          res.httpStatus shouldBe StatusCodes.OK
        }
      }
    }
  }

  val dataSourceSettings =
    Map(
      "source" ->
        FileSystemSourceSettings("test-source", "https://aiccelerate.eu/data-integration-suite/test-data", Paths.get(getClass.getResource("/test-data").toURI).normalize().toAbsolutePath.toString)
    )
  val fhirSinkSettings: FhirRepositorySinkSettings = FhirRepositorySinkSettings(fhirRepoUrl = "http://localhost:8081/fhir", writeErrorHandling = MappingErrorHandling.CONTINUE)

  val patientMappingTask: FhirMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/patient-mapping",
    sourceContext = Map("source" -> FileSystemSource(path = "patients.csv"))
  )
  val otherObservationMappingTask: FhirMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/other-observation-mapping",
    sourceContext = Map("source" -> FileSystemSource(path = "other-observations.csv"))
  )
  implicit val actorSystem: ActorSystem = ActorSystem("FhirMappingJobManagerTest")
  val onFhirClient: OnFhirNetworkClient = OnFhirNetworkClient.apply(fhirSinkSettings.fhirRepoUrl)
  val fhirServerIsAvailable: Boolean =
    Try(Await.result(onFhirClient.search("Patient").execute(), FiniteDuration(5, TimeUnit.SECONDS)).httpStatus == StatusCodes.OK)
      .getOrElse(false)

  val testMappingJobFilePath: String = getClass.getResource("/test-mappingjob.json").toURI.getPath

  val fhirMappingJob: FhirMappingJob =
    FhirMappingJob(
      id = "test-mapping-job",
      schedulingSettings = Option.empty,
      sourceSettings = dataSourceSettings,
      sinkSettings = fhirSinkSettings,
      mappings = Seq(
      patientMappingTask,
      otherObservationMappingTask
      //FileSourceMappingDefinition(patientMappingTask.mappingRef, patientMappingTask.sourceContext("source").asInstanceOf[FileSystemSource].path),
      //FileSourceMappingDefinition(otherObservationMappingTask.mappingRef, otherObservationMappingTask.sourceContext("source").asInstanceOf[FileSystemSource].path)
      ),
    mappingErrorHandling = MappingErrorHandling.CONTINUE)

  "A FhirMappingJobManager" should "execute the patient mapping task and return the results" in {
    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, sparkSession, mappingErrorHandling)
    fhirMappingJobManager.executeMappingTaskAndReturn(task = patientMappingTask, sourceSettings =  dataSourceSettings) map { results =>
      results.size shouldBe 10
      val patient1 = results.head
      FHIRUtil.extractResourceType(patient1) shouldBe "Patient"
      FHIRUtil.extractIdFromResource(patient1) shouldBe FhirMappingUtility.getHashedId("Patient", "p1")
      FHIRUtil.extractValue[String](patient1, "gender") shouldBe "male"

      val patient2 = results(1)
      FHIRUtil.extractResourceType(patient2) shouldBe "Patient"
      FHIRUtil.extractIdFromResource(patient2) shouldBe FhirMappingUtility.getHashedId("Patient", "p2")
      FHIRUtil.extractValue[String](patient2, "deceasedDateTime") shouldBe "2017-03-10"

      val patient10 = results.last
      FHIRUtil.extractResourceType(patient10) shouldBe "Patient"
      FHIRUtil.extractIdFromResource(patient10) shouldBe FhirMappingUtility.getHashedId("Patient", "p10")
      FHIRUtil.extractValue[String](patient10, "gender") shouldBe "female"
      FHIRUtil.extractValue[String](patient10, "birthDate") shouldBe "2003-11"
      FHIRUtil.extractValueOption[String](patient10, "deceasedDateTime").isEmpty shouldBe true
    }
  }

  it should "execute the other observation mapping task and return the results" in {
    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, sparkSession, mappingErrorHandling)
    fhirMappingJobManager.executeMappingTaskAndReturn(task = otherObservationMappingTask, sourceSettings =  dataSourceSettings) map { results =>
      results.size shouldBe 14
      val observation1 = results.head
      FHIRUtil.extractResourceType(observation1) shouldBe "Observation"
      (observation1 \ "subject" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Patient", "p1")

      val observation5 = results(4)
      FHIRUtil.extractResourceType(observation5) shouldBe "Observation"
      (observation5 \ "valueQuantity" \ "value").extract[Double] shouldBe 43.2
      (observation5 \ "meta" \ "profile").extract[Seq[String]].head shouldBe "https://aiccelerate.eu/fhir/StructureDefinition/AIC-IntraOperativeObservation"
      (observation5 \ "valueQuantity" \ "unit").extract[String] shouldBe "mL"
      (observation5 \ "code" \ "coding" \ "code").extract[Seq[String]].head shouldBe "1298-9"
      (observation5 \ "code" \ "coding" \ "display").extract[Seq[String]].head shouldBe "RBC given"

      (results(8) \ "meta" \ "profile").extract[Seq[String]].head shouldBe "https://aiccelerate.eu/fhir/StructureDefinition/AIC-PEWSScore"
      (results(10) \ "meta" \ "profile").extract[Seq[String]].head shouldBe "https://aiccelerate.eu/fhir/StructureDefinition/AIC-MedicationAdministration"

      (results(13) \ "component" \ "valueQuantity" \ "value").extract[Seq[Int]] shouldBe Seq(3, 5, 4)
      (results(13) \ "valueQuantity" \ "value").extract[Int] shouldBe 4
    }
  }


  it should "execute the mapping job with multiple mapping tasks and write the results into a FHIR repository" in {
    assume(fhirServerIsAvailable)

    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, sparkSession, mappingErrorHandling)
    fhirMappingJobManager.executeMappingJob(tasks = Seq(patientMappingTask, otherObservationMappingTask), sourceSettings =  dataSourceSettings, sinkSettings = fhirSinkSettings) flatMap { response =>
      onFhirClient.read("Patient", FhirMappingUtility.getHashedId("Patient", "p8")).executeAndReturnResource() flatMap { p1Resource =>
        FHIRUtil.extractIdFromResource(p1Resource) shouldBe FhirMappingUtility.getHashedId("Patient", "p8")
        FHIRUtil.extractValue[String](p1Resource, "gender") shouldBe "female"
        FHIRUtil.extractValue[String](p1Resource, "birthDate") shouldBe "2010-01-10"

        onFhirClient.search("Observation").where("code", "1035-5").executeAndReturnBundle() flatMap { observationBundle =>
          (observationBundle.searchResults.head \ "subject" \ "reference").extract[String] shouldBe
            FhirMappingUtility.getHashedReference("Patient", "p1")

          onFhirClient.search("MedicationAdministration").where("code", "313002").executeAndReturnBundle() flatMap { medicationAdministrationBundle =>
            (medicationAdministrationBundle.searchResults.head \ "subject" \ "reference").extract[String] shouldBe
              FhirMappingUtility.getHashedReference("Patient", "p4")
            deleteResources()
          }
        }
      }
    }
  }

  it should "save and read FhirMappingJob objects to/from a file" in {
    val lFileName = "tmp-mappingjob.json"
    FhirMappingJobManager.saveMappingJobToFile(fhirMappingJob, lFileName)
    val f = new File(lFileName)
    f.exists() shouldBe true

    val lMappingJobs = FhirMappingJobManager.readMappingJobFromFile(lFileName)
    lMappingJobs.mappings.size shouldBe 2
    f.delete() shouldBe true
  }

  it should "execute the FhirMappingJob restored from a file" in {
    val lMappingJob = FhirMappingJobManager.readMappingJobFromFile(testMappingJobFilePath)

    // I do the following dirty thing because our data reading mechanism should both handle the relative paths while running and while testing.
    val lTask = lMappingJob.mappings.head

    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, new MappingContextLoader(mappingRepository), schemaRepository, sparkSession, lMappingJob.mappingErrorHandling)
    fhirMappingJobManager.executeMappingTaskAndReturn(task = lTask, sourceSettings = lMappingJob.sourceSettings) map { results =>
      results.size shouldBe 10
    }
  }

  it should "execute the FhirMappingJob with sink settings restored from a file" in {
    assume(fhirServerIsAvailable)
    val lMappingJob = FhirMappingJobManager.readMappingJobFromFile(testMappingJobFilePath)

    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, new MappingContextLoader(mappingRepository), schemaRepository, sparkSession, lMappingJob.mappingErrorHandling)
    fhirMappingJobManager.executeMappingJob(tasks = lMappingJob.mappings, sourceSettings = lMappingJob.sourceSettings, sinkSettings = lMappingJob.sinkSettings) flatMap { unit =>
      unit shouldBe()
      deleteResources()
    }
  }

}