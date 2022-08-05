package io.onfhir.tofhir.engine

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import io.onfhir.api.client.FhirBatchTransactionRequestBuilder
import io.onfhir.api.util.FHIRUtil
import io.onfhir.client.OnFhirNetworkClient
import io.onfhir.tofhir.ToFhirTestSpec
import io.onfhir.tofhir.config.MappingErrorHandling.MappingErrorHandling
import io.onfhir.tofhir.config.{MappingErrorHandling, ToFhirConfig}
import io.onfhir.tofhir.model._
import io.onfhir.tofhir.util.{FhirMappingJobFormatter, FhirMappingUtility}
import io.onfhir.util.JsonFormatter.formats
import it.sauronsoftware.cron4j.Scheduler
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.Assertion
import org.scalatest.flatspec.AnyFlatSpec

import java.io.File
import java.net.URI
import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{Await, Future}
import scala.util.Try

class SchedulingTest extends AnyFlatSpec with ToFhirTestSpec {

  private def deleteResources(): Future[Assertion] = {
    var batchRequest: FhirBatchTransactionRequestBuilder = onFhirClient.batch()
    // Delete all patients between p1-p10 and related observations
    val obsSearchFutures = (1 to 10).map(i => {
      batchRequest = batchRequest.entry(_.delete("Patient", FhirMappingUtility.getHashedId("Patient", "p" + i)))
      onFhirClient.search("Observation").where("subject", "Patient/" + FhirMappingUtility.getHashedId("Patient", "p" + i))
        .executeAndReturnBundle()
    })
    Future.sequence(obsSearchFutures) flatMap { obsBundleList =>
      obsBundleList.foreach(observationBundle => {
        observationBundle.searchResults.foreach(obs =>
          batchRequest = batchRequest.entry(_.delete("Observation", (obs \ "id").extract[String]))
        )
      })
      // delete MedicationAdministration records by patient
      val medicationAdminSearchFutures = List(1, 2, 4).map(i => {
        batchRequest = batchRequest.entry(_.delete("Patient", FhirMappingUtility.getHashedId("Patient", "p" + i)))
        onFhirClient.search("MedicationAdministration").where("subject", "Patient/" + FhirMappingUtility.getHashedId("Patient", "p" + i))
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

  val scheduler = new Scheduler()
  val mappingJobSyncTimesURI: URI = getClass.getResource(ToFhirConfig.mappingJobSyncTimesFolderPath.get).toURI
  val mappingJobScheduler: MappingJobScheduler = MappingJobScheduler(scheduler, mappingJobSyncTimesURI)

  val dataSourceSettings: Map[String, FileSystemSourceSettings] =
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
  implicit val actorSystem: ActorSystem = ActorSystem("SchedulingTest")
  val onFhirClient: OnFhirNetworkClient = OnFhirNetworkClient.apply(fhirSinkSettings.fhirRepoUrl)
  val fhirServerIsAvailable: Boolean =
    Try(Await.result(onFhirClient.search("Patient").execute(), FiniteDuration(5, TimeUnit.SECONDS)).httpStatus == StatusCodes.OK)
      .getOrElse(false)

  val testScheduleMappingJobFilePath: String = getClass.getResource("/test-schedule-mappingjob.json").toURI.getPath

  it should "schedule a FhirMappingJob with cron and sink settings restored from a file" in {
    assume(fhirServerIsAvailable)
    val lMappingJob: FhirMappingJob = FhirMappingJobFormatter.readMappingJobFromFile(testScheduleMappingJobFilePath)

    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, new MappingContextLoader(mappingRepository), schemaRepository, sparkSession, lMappingJob.mappingErrorHandling, Some(mappingJobScheduler))
    fhirMappingJobManager.scheduleMappingJob(tasks = lMappingJob.mappings, sourceSettings = dataSourceSettings, sinkSettings = lMappingJob.sinkSettings, schedulingSettings = lMappingJob.schedulingSettings.get)
    scheduler.start() //job set to run every minute
    Thread.sleep(60000) //wait for the job to be executed once

    val directory = new File(mappingJobSyncTimesURI)
    FileUtils.cleanDirectory(directory)

    val searchTest = onFhirClient.read("Patient", FhirMappingUtility.getHashedId("Patient", "p8")).executeAndReturnResource() flatMap { p1Resource =>
      FHIRUtil.extractIdFromResource(p1Resource) shouldBe FhirMappingUtility.getHashedId("Patient", "p8")
      FHIRUtil.extractValue[String](p1Resource, "gender") shouldBe "female"
      FHIRUtil.extractValue[String](p1Resource, "birthDate") shouldBe "2010-01-10"

      onFhirClient.search("Observation").where("code", "1035-5").executeAndReturnBundle() flatMap { observationBundle =>
        (observationBundle.searchResults.head \ "subject" \ "reference").extract[String] shouldBe
          FhirMappingUtility.getHashedReference("Patient", "p1")

        onFhirClient.search("MedicationAdministration").where("code", "313002").executeAndReturnBundle() map { medicationAdministrationBundle =>
          (medicationAdministrationBundle.searchResults.head \ "subject" \ "reference").extract[String] shouldBe
            FhirMappingUtility.getHashedReference("Patient", "p4")
          deleteResources()
        }
      }
    }
    Await.result(searchTest, Duration.Inf)
  }


}

