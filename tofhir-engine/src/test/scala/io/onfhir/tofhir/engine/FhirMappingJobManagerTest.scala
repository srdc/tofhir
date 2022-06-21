package io.onfhir.tofhir.engine

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import com.typesafe.scalalogging.Logger
import io.onfhir.api.util.FHIRUtil
import io.onfhir.client.OnFhirNetworkClient
import io.onfhir.tofhir.ToFhirTestSpec
import io.onfhir.tofhir.config.MappingErrorHandling
import io.onfhir.tofhir.model._
import io.onfhir.tofhir.util.FhirMappingUtility
import io.onfhir.util.JsonFormatter.formats
import it.sauronsoftware.cron4j.{Scheduler, SchedulerListener, TaskExecutor}

import java.io.File
import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

class FhirMappingJobManagerTest extends ToFhirTestSpec {

  private val logger: Logger = Logger(this.getClass)


  val dataSourceSettings: FileSystemSourceSettings = FileSystemSourceSettings("test-source", "https://aiccelerate.eu/data-integration-suite/test-data",
    Paths.get(getClass.getResource("/test-data").toURI).normalize().toAbsolutePath.toString)
  val fhirSinkSettings: FhirRepositorySinkSettings = FhirRepositorySinkSettings(fhirRepoUrl = "http://localhost:8081/fhir", writeErrorHandling = MappingErrorHandling.CONTINUE)

  val patientMappingTask: FhirMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/patient-mapping",
    sourceContext = Map("source" -> FileSystemSource(path = "patients.csv", sourceType = SourceFileFormats.CSV, dataSourceSettings))
  )
  val otherObservationMappingTask: FhirMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/other-observation-mapping",
    sourceContext = Map("source" -> FileSystemSource(path = "other-observations.csv", sourceType = SourceFileFormats.CSV, settings = dataSourceSettings))
  )
  implicit val actorSystem: ActorSystem = ActorSystem("FhirMappingJobManagerTest")
  val onFhirClient: OnFhirNetworkClient = OnFhirNetworkClient.apply(fhirSinkSettings.fhirRepoUrl)
  val fhirServerIsAvailable: Boolean =
    Try(Await.result(onFhirClient.search("Patient").execute(), FiniteDuration(5, TimeUnit.SECONDS)).httpStatus == StatusCodes.OK)
      .getOrElse(false)

  val testMappingJobFilePath: String = getClass.getResource("/test-mappingjob.json").toURI.getPath
  val testScheduleMappingJobFilePath: String = getClass.getResource("/test-schedule-mappingjob.json").toURI.getPath

  val fhirMappingJob: FhirMappingJob = FhirMappingJob(id = "test-mapping-job",
    cronExpression = Option.empty,
    sourceSettings = dataSourceSettings,
    sinkSettings = fhirSinkSettings,
    mappings = Seq(FileSourceMappingDefinition(patientMappingTask.mappingRef, patientMappingTask.sourceContext("source").asInstanceOf[FileSystemSource].path),
      FileSourceMappingDefinition(otherObservationMappingTask.mappingRef, otherObservationMappingTask.sourceContext("source").asInstanceOf[FileSystemSource].path)),
    mappingErrorHandling = MappingErrorHandling.CONTINUE)

  "A FhirMappingJobManager" should "execute the patient mapping task and return the results" in {
    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, sparkSession, mappingErrorHandling)
    fhirMappingJobManager.executeMappingTaskAndReturn(task = patientMappingTask) map { results =>
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
    fhirMappingJobManager.executeMappingTaskAndReturn(task = otherObservationMappingTask) map { results =>
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
    fhirMappingJobManager.executeMappingJob(tasks = Seq(patientMappingTask, otherObservationMappingTask), sinkSettings = fhirSinkSettings) flatMap { response =>
      onFhirClient.read("Patient", FhirMappingUtility.getHashedId("Patient", "p8")).executeAndReturnResource() flatMap { p1Resource =>
        FHIRUtil.extractIdFromResource(p1Resource) shouldBe FhirMappingUtility.getHashedId("Patient", "p8")
        FHIRUtil.extractValue[String](p1Resource, "gender") shouldBe "female"
        FHIRUtil.extractValue[String](p1Resource, "birthDate") shouldBe "2010-01-10"

        onFhirClient.search("Observation").where("code", "1035-5").executeAndReturnBundle() flatMap { observationBundle =>
          (observationBundle.searchResults.head \ "subject" \ "reference").extract[String] shouldBe
            FhirMappingUtility.getHashedReference("Patient", "p1")

          onFhirClient.search("MedicationAdministration").where("code", "313002").executeAndReturnBundle() map { medicationAdministrationBundle =>
            (medicationAdministrationBundle.searchResults.head \ "subject" \ "reference").extract[String] shouldBe
              FhirMappingUtility.getHashedReference("Patient", "p4")
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
    lMappingJobs.tasks.size shouldBe 2
    f.delete() shouldBe true
  }

  it should "execute the FhirMappingJob restored from a file" in {
    val lMappingJob = FhirMappingJobManager.readMappingJobFromFile(testMappingJobFilePath)

    // I do the following dirty thing because our data reading mechanism should both handle the relative paths while running and while testing.
    val lTask = lMappingJob.tasks.head
    val lFileSystemSource = lTask.sourceContext("source").asInstanceOf[FileSystemSource]
    val task = FhirMappingTask(lTask.mappingRef,
      Map("source" -> FileSystemSource(lFileSystemSource.path, lFileSystemSource.sourceType,
        FileSystemSourceSettings(lFileSystemSource.settings.name, lFileSystemSource.settings.sourceUri,
          Paths.get(getClass.getResource(lFileSystemSource.settings.dataFolderPath).toURI).normalize().toAbsolutePath.toString))))

    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, new MappingContextLoader(mappingRepository), schemaRepository, sparkSession, lMappingJob.mappingErrorHandling)
    fhirMappingJobManager.executeMappingTaskAndReturn(task = task) map { results =>
      results.size shouldBe 10
    }
  }

  it should "execute the FhirMappingJob with sink settings restored from a file" in {
    assume(fhirServerIsAvailable)
    val lMappingJob = FhirMappingJobManager.readMappingJobFromFile(testMappingJobFilePath)

    // I do the following dirty thing because our data reading mechanism should both handle the relative paths while running and while testing.
    val tasks = lMappingJob.tasks.map { task =>
      val lFileSystemSource = task.sourceContext("source").asInstanceOf[FileSystemSource]
      FhirMappingTask(task.mappingRef,
        Map("source" -> FileSystemSource(lFileSystemSource.path, lFileSystemSource.sourceType,
          FileSystemSourceSettings(lFileSystemSource.settings.name, lFileSystemSource.settings.sourceUri,
            Paths.get(getClass.getResource(lFileSystemSource.settings.dataFolderPath).toURI).normalize().toAbsolutePath.toString))))
    }

    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, new MappingContextLoader(mappingRepository), schemaRepository, sparkSession, lMappingJob.mappingErrorHandling)
    fhirMappingJobManager.executeMappingJob(tasks = tasks, sinkSettings = lMappingJob.sinkSettings) map { unit =>
      unit shouldBe()
    }
  }

  it should "schedule a FhirMappingJob with cron and sink settings restored from a file" in {
    assume(fhirServerIsAvailable)
    val lMappingJob: FhirMappingJob = FhirMappingJobManager.readMappingJobFromFile(testScheduleMappingJobFilePath)

    // I do the following dirty thing because our data reading mechanism should both handle the relative paths while running and while testing.
    val tasks: Seq[FhirMappingTask] = lMappingJob.tasks.map { task =>
      val lFileSystemSource = task.sourceContext("source").asInstanceOf[FileSystemSource]
      FhirMappingTask(task.mappingRef,
        Map("source" -> FileSystemSource(lFileSystemSource.path, lFileSystemSource.sourceType,
          FileSystemSourceSettings(lFileSystemSource.settings.name, lFileSystemSource.settings.sourceUri,
            Paths.get(getClass.getResource(lFileSystemSource.settings.dataFolderPath).toURI).normalize().toAbsolutePath.toString))))
    }


    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, new MappingContextLoader(mappingRepository), schemaRepository, sparkSession, lMappingJob.mappingErrorHandling)
    val scheduler = fhirMappingJobManager.scheduleMappingJob(tasks = tasks, sinkSettings = lMappingJob.sinkSettings, cronExpression = lMappingJob.cronExpression.get)
    var jobCompleted = false

    val schedulerListener = new SchedulerListener {

      def taskLaunching(executor: TaskExecutor): Unit = {
        logger.info(s"Scheduled mapping job launched: ${executor.getTask}")
      }

      def taskSucceeded(executor: TaskExecutor): Unit = {
        logger.info("Scheduled mapping job completed!")
        jobCompleted = true
      }

      def taskFailed(executor: TaskExecutor, exception: Throwable): Unit = {
        logger.info("Scheduled mapping job failed due to an exception!")
        exception.printStackTrace()
        jobCompleted = true
      }
    }

    scheduler.addSchedulerListener(schedulerListener)
    while (!jobCompleted) {
      Thread.sleep(1000)
    }

    scheduler shouldBe a[Scheduler]
  }
}