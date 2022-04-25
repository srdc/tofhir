package io.onfhir.tofhir.engine

import akka.http.scaladsl.model.StatusCodes
import io.onfhir.api.util.FHIRUtil
import io.onfhir.client.OnFhirNetworkClient
import io.onfhir.tofhir.ToFhirTestSpec
import io.onfhir.tofhir.model._
import io.onfhir.tofhir.util.FhirMappingUtility
import io.onfhir.util.JsonFormatter.formats
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.io.File
import java.net.URI
import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

class FhirMappingJobManagerTest extends ToFhirTestSpec {

  val sparkConf: SparkConf = new SparkConf()
    .setAppName("tofhir-test")
    .setMaster("local")
    .set("spark.driver.allowMultipleContexts", "false")
    .set("spark.ui.enabled", "false")
  val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  val dataSourceSettings: FileSystemSourceSettings = FileSystemSourceSettings("test-source-1", "file:///test-source-1", getClass.getResource("/test-data-1").toURI)
  val fhirSinkSettings: FhirRepositorySinkSettings = FhirRepositorySinkSettings("http://localhost:8081/fhir")

  val patientMappingTask: FhirMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/patient-mapping",
    sourceContext = Map("source" -> FileSystemSource(path = "patients.csv", sourceType = SourceFileFormats.CSV, dataSourceSettings))
  )
  val otherObservationMappingTask: FhirMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/other-observation-mapping",
    sourceContext = Map("source" -> FileSystemSource(path = "other-observations.csv", sourceType = SourceFileFormats.CSV, settings = dataSourceSettings))
  )

  val mappingRepositoryURI: URI = getClass.getResource("/test-mappings-1").toURI
  val mappingRepository: IFhirMappingRepository =
    new FhirMappingFolderRepository(mappingRepositoryURI)
  val contextLoader: IMappingContextLoader = new MappingContextLoader(mappingRepository)
  val schemaRepositoryURI: URI = getClass.getResource("/test-schema").toURI
  val schemaRepository = new SchemaFolderRepository(schemaRepositoryURI)

  val onFhirClient: OnFhirNetworkClient = OnFhirNetworkClient.apply(fhirSinkSettings.fhirRepoUrl)
  val fhirServerIsAvailable: Boolean =
    Try(Await.result(onFhirClient.search("Patient").execute(), FiniteDuration(5, TimeUnit.SECONDS)).httpStatus == StatusCodes.OK)
      .getOrElse(false)

  val testMappingJobFilePath: String = Paths.get(getClass.getResource("/test-mappingjob.json").toURI).normalize().toAbsolutePath.toString
  val fhirMappingJob: FhirMappingJob = FhirMappingJob(id = "test-mapping-job",
    mappingRepositoryUri = mappingRepositoryURI,
    schemaRepositoryUri = schemaRepositoryURI,
    sinkSettings = fhirSinkSettings,
    tasks = Seq(patientMappingTask, otherObservationMappingTask))

  "A FhirMappingJobManager" should "execute the patient mapping task and return the results" in {
    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, sparkSession)
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
    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, sparkSession)
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

    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, sparkSession)
    fhirMappingJobManager.executeMappingJob(tasks = Seq(patientMappingTask, otherObservationMappingTask), sinkSettings = fhirSinkSettings) flatMap { response =>
      onFhirClient.read("Patient", FhirMappingUtility.getHashedId("Patient", "p8")).executeAndReturnResource() flatMap { p1Resource =>
        FHIRUtil.extractIdFromResource(p1Resource) shouldBe FhirMappingUtility.getHashedId("Patient", "p8")
        FHIRUtil.extractValue[String](p1Resource, "gender") shouldBe "female"
        FHIRUtil.extractValue[String](p1Resource, "birthDate") shouldBe "2010-01-10"

        onFhirClient.search("Observation").where("code", "1035-5").executeAndReturnBundle() flatMap { observationBundle =>
          observationBundle.searchResults.size shouldBe 1
          (observationBundle.searchResults.head \ "subject" \ "reference").extract[String] shouldBe
            FhirMappingUtility.getHashedReference("Patient", "p1")

          onFhirClient.search("MedicationAdministration").where("code", "313002").executeAndReturnBundle() map { medicationAdministrationBundle =>
            medicationAdministrationBundle.searchResults.size shouldBe 1
            (medicationAdministrationBundle.searchResults.head \ "subject" \ "reference").extract[String] shouldBe
              FhirMappingUtility.getHashedReference("Patient", "p4")
          }
        }
      }
    }
  }

  it should "save and read FhirMappingJob objects to/from a file" in {
    val lFileName = "tmp-mappingjob.json"
    FhirMappingJobManager.saveMappingJobsToFile(Seq(fhirMappingJob), lFileName)
    val f = new File(lFileName)
    f.exists() shouldBe true

    val lMappingJobs = FhirMappingJobManager.readMappingJobFromFile(lFileName)
    lMappingJobs.size shouldBe 1
    lMappingJobs.head.tasks.size shouldBe 2
    f.delete() shouldBe true
  }

  it should "execute the FhirMappingJob restored from a file" in {
    val lMappingJobs = FhirMappingJobManager.readMappingJobFromFile(testMappingJobFilePath)
    Paths.get(lMappingJobs.head.mappingRepositoryUri).getFileName.toString shouldBe "test-mappings-1"
    Paths.get(lMappingJobs.head.schemaRepositoryUri).getFileName.toString shouldBe "test-schema"

    val lMappingRepository = new FhirMappingFolderRepository(mappingRepositoryURI)
    val lSchemaRepository = new SchemaFolderRepository(schemaRepositoryURI)
    val loadedTask = lMappingJobs.head.tasks.head
    val loadedFileSystemSource = loadedTask.sourceContext.head._2.asInstanceOf[FileSystemSource]
    val task = FhirMappingTask(mappingRef = loadedTask.mappingRef,
      sourceContext = Map(loadedTask.sourceContext.head._1 ->
        FileSystemSource(loadedFileSystemSource.path, loadedFileSystemSource.sourceType,
          FileSystemSourceSettings(loadedFileSystemSource.settings.name, loadedFileSystemSource.settings.sourceUri, getClass.getResource("/test-data-1").toURI))))

    val fhirMappingJobManager = new FhirMappingJobManager(lMappingRepository, new MappingContextLoader(lMappingRepository), lSchemaRepository, sparkSession)
    fhirMappingJobManager.executeMappingTaskAndReturn(task = task) map { results =>
      results.size shouldBe 10
    }
  }

  it should "execute the FhirMappingJob with sink settings restored from a file" in {
    assume(fhirServerIsAvailable)
    val lMappingJobs = FhirMappingJobManager.readMappingJobFromFile(testMappingJobFilePath)
    Paths.get(lMappingJobs.head.mappingRepositoryUri).getFileName.toString shouldBe "test-mappings-1"
    Paths.get(lMappingJobs.head.schemaRepositoryUri).getFileName.toString shouldBe "test-schema"

    val lMappingRepository = new FhirMappingFolderRepository(mappingRepositoryURI)
    val lSchemaRepository = new SchemaFolderRepository(schemaRepositoryURI)
    val loadedTask = lMappingJobs.head.tasks.head
    val loadedFileSystemSource = loadedTask.sourceContext.head._2.asInstanceOf[FileSystemSource]
    val task = FhirMappingTask(mappingRef = loadedTask.mappingRef,
      sourceContext = Map(loadedTask.sourceContext.head._1 ->
        FileSystemSource(loadedFileSystemSource.path, loadedFileSystemSource.sourceType,
          FileSystemSourceSettings(loadedFileSystemSource.settings.name, loadedFileSystemSource.settings.sourceUri, getClass.getResource("/test-data-1").toURI))))

    val fhirMappingJobManager = new FhirMappingJobManager(lMappingRepository, new MappingContextLoader(lMappingRepository), lSchemaRepository, sparkSession)
    fhirMappingJobManager.executeMappingJob(tasks = Seq(task), sinkSettings = lMappingJobs.head.sinkSettings) map { unit =>
      unit shouldBe()
    }
  }

}
