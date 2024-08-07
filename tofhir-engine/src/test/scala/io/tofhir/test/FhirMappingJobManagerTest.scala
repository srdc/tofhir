package io.tofhir.test

import akka.http.scaladsl.model.StatusCodes
import io.onfhir.api.Resource
import io.onfhir.api.client.FhirBatchTransactionRequestBuilder
import io.onfhir.api.util.FHIRUtil
import io.onfhir.path.{FhirPathIdentityServiceFunctionsFactory, FhirPathUtilFunctionsFactory}
import io.onfhir.util.JsonFormatter._
import io.tofhir.engine.mapping.context.MappingContextLoader
import io.tofhir.{OnFhirTestContainer, ToFhirTestSpec}
import io.tofhir.engine.mapping.job.FhirMappingJobManager
import io.tofhir.engine.model._
import io.tofhir.engine.model.exception.FhirMappingException
import io.tofhir.engine.util.{FhirMappingJobFormatter, FhirMappingUtility, FileUtils}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.{Assertion, BeforeAndAfterAll}

import java.io.File
import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext}

class FhirMappingJobManagerTest extends AsyncFlatSpec with BeforeAndAfterAll with ToFhirTestSpec with OnFhirTestContainer {

  val dataSourceSettings: Map[String, DataSourceSettings] =
    Map("source" ->
      FileSystemSourceSettings("test-source", "https://aiccelerate.eu/data-integration-suite/test-data", Paths.get(getClass.getResource("/test-data").toURI).normalize().toAbsolutePath.toString))
  val fhirSinkSettings: FhirRepositorySinkSettings = FhirRepositorySinkSettings(fhirRepoUrl = onFhirClient.getBaseUrl())

  val patientMappingTask: FhirMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/patient-mapping",
    sourceContext = Map("source" -> FileSystemSource(path = "patients.csv"))
  )

  val patientMappingTaskWithError: FhirMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/patient-mapping",
    sourceContext = Map("source" -> FileSystemSource(path = "patients-erroneous.csv"))
  )

  val patientMappingTaskWithPreprocess: FhirMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/patient-mapping",
    sourceContext = Map("source" ->
      FileSystemSource(
        path = "patients-column-renamed.csv",
        preprocessSql = Some(
          "SELECT pid,sex as gender,to_date(birth) as birthDate,NULL as deceasedDateTime,NULL as homePostalCode FROM source"))
    )
  )

  val otherObservationMappingTask: FhirMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/other-observation-mapping",
    sourceContext = Map("source" -> FileSystemSource(path = "other-observations.csv"))
  )

  val patientExtraMappingWithPatch: FhirMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/patient-mapping-with-patch",
    sourceContext = Map("source" -> FileSystemSource(path = "patients-extra.csv"))
  )

  val patientExtraMappingWithConditionalPatch: FhirMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/patient-mapping-with-patch2",
    sourceContext = Map("source" -> FileSystemSource(path = "patients-extra.csv"))
  )

  val patientTsvFileMappingTask: FhirMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/patient-mapping",
    sourceContext = Map("source" -> FileSystemSource(path = "patients.tsv"))
  )

  val testMappingJobFilePath: String = getClass.getResource("/test-mappingjob.json").toURI.getPath
  val testMappingJobWithIdentityServiceFilePath: String = getClass.getResource("/test-mappingjob-using-services.json").toURI.getPath

  val fhirMappingJob: FhirMappingJob =
    FhirMappingJob(
      id = "test-mapping-job",
      sourceSettings = dataSourceSettings,
      sinkSettings = fhirSinkSettings,
      mappings = Seq(
        patientMappingTask,
        otherObservationMappingTask
      ),
      dataProcessingSettings = DataProcessingSettings())

  override protected def afterAll(): Unit = {
    deleteResources()
    // delete context path
    org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath("").toFile)
    super.afterAll()
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    // copy test data files to the context path so that 'execute a mapping job with two data sources' test can find
    // them while running the mapping job
    copyResourceFile("test-data/patient-simple.csv")
    copyResourceFile("test-data-gender/patient-gender-simple.csv")
  }

  private def deleteResources(): Assertion = {
    // define an implicit ExecutionContext which is required for FhirSearchRequestBuilder.executeAndReturnBundle method
    import io.tofhir.engine.Execution.actorSystem.dispatcher
    // Start delete operation of written resources on the FHIR
    var batchRequest: FhirBatchTransactionRequestBuilder = onFhirClient.batch()

    // delete test-patient
    batchRequest = batchRequest.entry(_.delete("Patient", "test-patient"))
    // Delete all patients between p1-p10 and related observations

    (1 to 10).foreach(i => {
      // add Delete Patient request to the batch
      batchRequest = batchRequest.entry(_.delete("Patient", FhirMappingUtility.getHashedId("Patient", "p" + i)))
      val patientReference = FhirMappingUtility.getHashedReference("Patient", "p" + i)
      // search for the patient's Observation and MedicationAdministration resources
      // and add Delete requests for these resources to the batch
      val f = onFhirClient.search("Observation").where("subject", patientReference).executeAndReturnBundle() map { observationBundle =>
        observationBundle.searchResults.foreach(obs =>
          batchRequest = batchRequest.entry(_.delete("Observation", (obs \ "id").extract[String]))
        )
        onFhirClient.search("MedicationAdministration").where("subject", patientReference).executeAndReturnBundle() map { medicationAdminBundle =>
          medicationAdminBundle.searchResults.foreach(mAdmin =>
            batchRequest = batchRequest.entry(_.delete("MedicationAdministration", (mAdmin \ "id").extract[String]))
          )
        }
      }
      Await.result(f, FiniteDuration(60, TimeUnit.SECONDS))
    })

    val res = Await.result(batchRequest.returnMinimal().asInstanceOf[FhirBatchTransactionRequestBuilder].execute(), FiniteDuration(60, TimeUnit.SECONDS))
    res.httpStatus shouldBe StatusCodes.OK
  }

  it should "execute the patient mapping task and return the results" in {
    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, Map.empty, sparkSession)
    fhirMappingJobManager.executeMappingTaskAndReturn(mappingJobExecution = FhirMappingJobExecution(
      job = fhirMappingJob,
      mappingTasks = Seq(patientMappingTask)) , sourceSettings = dataSourceSettings) map { mappingResults =>
      val results = mappingResults.map(r => {
        r.mappedResource should not be None
        val resource = r.mappedResource.get.parseJson
        resource shouldBe a[Resource]
        resource
      })

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

  it should "execute a mapping job with two data sources" in {
    val mappingJob = FhirMappingJobFormatter.readMappingJobFromFile(getClass.getResource("/patient-mapping-job-with-two-sources.json").toURI.getPath)

    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, Map.empty, sparkSession)
    fhirMappingJobManager.executeMappingJob(mappingJobExecution = FhirMappingJobExecution(mappingTasks = mappingJob.mappings, job = mappingJob), sourceSettings = mappingJob.sourceSettings, sinkSettings = mappingJob.sinkSettings.asInstanceOf[FhirRepositorySinkSettings].copy(fhirRepoUrl = onFhirClient.getBaseUrl())) flatMap { _ =>
      onFhirClient.read("Patient", "test-patient").executeAndReturnResource() flatMap { p1Resource =>
        (p1Resource \ "id").extract[String] shouldBe "test-patient"
        (p1Resource \ "gender").extract[String] shouldBe "male"
      }
    }
  }

  it should "execute the mappings with FHIR Path patch" in {
    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, Map.empty, sparkSession)
    fhirMappingJobManager.executeMappingJob(mappingJobExecution = FhirMappingJobExecution(mappingTasks = Seq(patientMappingTask), job = fhirMappingJob), sourceSettings = dataSourceSettings, sinkSettings = fhirSinkSettings).flatMap(_ =>
      fhirMappingJobManager.executeMappingJob(mappingJobExecution = FhirMappingJobExecution(mappingTasks = Seq(patientExtraMappingWithPatch), job = fhirMappingJob) , sourceSettings = dataSourceSettings, sinkSettings = fhirSinkSettings) flatMap { response =>
        onFhirClient.read("Patient", FhirMappingUtility.getHashedId("Patient", "p1")).executeAndReturnResource() flatMap { p1Resource =>
          onFhirClient.read("Patient", FhirMappingUtility.getHashedId("Patient", "p2")).executeAndReturnResource() flatMap { p2Resource =>
            (p1Resource \ "maritalStatus" \ "coding" \ "code").extract[Seq[String]] shouldBe Seq("D")
            (p1Resource \ "gender").extract[String] shouldBe "male"

            (p2Resource \ "maritalStatus" \ "coding" \ "code").extract[Seq[String]] shouldBe Seq("M")
            (p2Resource \ "gender").extract[String] shouldBe "female"
          }
        }
      }
    )
  }

  it should "execute the mappings with conditional FHIR Path patch" in {
    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, Map.empty, sparkSession)
    fhirMappingJobManager.executeMappingJob(mappingJobExecution = FhirMappingJobExecution(mappingTasks = Seq(patientMappingTask), job = fhirMappingJob), sourceSettings = dataSourceSettings, sinkSettings = fhirSinkSettings).flatMap(_ =>
      fhirMappingJobManager.executeMappingJob(mappingJobExecution = FhirMappingJobExecution(mappingTasks = Seq(patientExtraMappingWithConditionalPatch), job = fhirMappingJob), sourceSettings = dataSourceSettings, sinkSettings = fhirSinkSettings) flatMap { response =>
        response shouldBe()
        onFhirClient.read("Patient", FhirMappingUtility.getHashedId("Patient", "p1")).executeAndReturnResource() flatMap { p1Resource =>
          onFhirClient.read("Patient", FhirMappingUtility.getHashedId("Patient", "p2")).executeAndReturnResource() flatMap { p2Resource =>
            (p1Resource \ "communication" \ "language" \ "coding" \ "code").extract[Seq[String]] shouldBe Seq("tr")
            (p2Resource \ "communication" \ "language" \ "coding" \ "code").extract[Seq[String]] shouldBe Seq("en")
            fhirMappingJobManager.executeMappingJob(mappingJobExecution = FhirMappingJobExecution(mappingTasks = Seq(patientExtraMappingWithConditionalPatch), job = fhirMappingJob), sourceSettings = dataSourceSettings, sinkSettings = fhirSinkSettings) flatMap { response =>
              onFhirClient.read("Patient", FhirMappingUtility.getHashedId("Patient", "p1")).executeAndReturnResource() flatMap { p1Resource =>
                onFhirClient.read("Patient", FhirMappingUtility.getHashedId("Patient", "p2")).executeAndReturnResource() flatMap { p2Resource =>
                  (p1Resource \ "communication" \ "language" \ "coding" \ "code").extract[Seq[String]] shouldBe Seq("tr")
                  (p2Resource \ "communication" \ "language" \ "coding" \ "code").extract[Seq[String]] shouldBe Seq("en")
                }
              }
            }
          }
        }
      }
    )
  }

  it should "execute the patient mapping task with given tsv file and return the results" in {
    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, Map.empty, sparkSession)
    fhirMappingJobManager.executeMappingTaskAndReturn(mappingJobExecution = FhirMappingJobExecution(
      job = fhirMappingJob,
      mappingTasks = Seq(patientTsvFileMappingTask)), sourceSettings = dataSourceSettings) map { mappingResults =>
      val results = mappingResults.map(r => {
        r.mappedResource should not be None
        val resource = r.mappedResource.get.parseJson
        resource shouldBe a[Resource]
        resource
      })

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
    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, Map(FhirPathUtilFunctionsFactory.defaultPrefix -> FhirPathUtilFunctionsFactory), sparkSession)
    fhirMappingJobManager.executeMappingTaskAndReturn(mappingJobExecution = FhirMappingJobExecution(mappingTasks = Seq(otherObservationMappingTask), job = fhirMappingJob) , sourceSettings = dataSourceSettings) map { mappingResults =>
      val results = mappingResults.map(r => {
        r.mappedResource should not be None
        val resource = r.mappedResource.get.parseJson
        resource shouldBe a[Resource]
        resource
      })
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
    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, Map(FhirPathUtilFunctionsFactory.defaultPrefix -> FhirPathUtilFunctionsFactory), sparkSession)
    fhirMappingJobManager.executeMappingJob(mappingJobExecution = FhirMappingJobExecution(mappingTasks = Seq(patientMappingTask, otherObservationMappingTask), job = fhirMappingJob), sourceSettings = dataSourceSettings, sinkSettings = fhirSinkSettings) flatMap { response =>
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
          }
        }
      }
    }
  }

  it should "continue execute the mapping job when encounter without an error" in {
    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, Map(FhirPathUtilFunctionsFactory.defaultPrefix -> FhirPathUtilFunctionsFactory), sparkSession)

    val future = fhirMappingJobManager.executeMappingJob(mappingJobExecution = FhirMappingJobExecution(
      mappingTasks = Seq(patientMappingTaskWithError, otherObservationMappingTask),
      job = fhirMappingJob),
      sourceSettings = dataSourceSettings,
      sinkSettings = fhirSinkSettings)
    try {
      Await.result(future, Duration.apply("5000 ms"))
      succeed
    }catch{
      case t: Throwable => t shouldNot be (a[FhirMappingException])
    }
  }

  it should "save and read FhirMappingJob objects to/from a file" in {
    val lFileName = "tmp-mappingjob.json"
    FhirMappingJobFormatter.saveMappingJobToFile(fhirMappingJob, lFileName)
    val f = new File(lFileName)
    f.exists() shouldBe true

    val lMappingJobs = FhirMappingJobFormatter.readMappingJobFromFile(lFileName)
    lMappingJobs.mappings.size shouldBe 2
    f.delete() shouldBe true
  }

  it should "execute the FhirMappingJob restored from a file" in {
    val lMappingJob = FhirMappingJobFormatter.readMappingJobFromFile(testMappingJobFilePath)

    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, new MappingContextLoader, schemaRepository, Map.empty, sparkSession)
    fhirMappingJobManager.executeMappingTaskAndReturn(mappingJobExecution = FhirMappingJobExecution(mappingTasks = Seq(lMappingJob.mappings.head), job = fhirMappingJob) , sourceSettings = dataSourceSettings) map { results =>
      results.size shouldBe 10
    }
  }

  it should "execute the FhirMapping task with a terminology service" in {
    val terminologyServiceFolderPath = Paths.get(getClass.getResource("/terminology-service").toURI).normalize().toAbsolutePath.toString
    val terminologyServiceSettings = LocalFhirTerminologyServiceSettings(terminologyServiceFolderPath,
      conceptMapFiles = Seq(
        ConceptMapFile("sample-concept-map.csv", "sample-concept-map.csv", "http://example.com/fhir/ConceptMap/sample1", "http://terminology.hl7.org/ValueSet/v2-0487", "http://snomed.info/sct?fhir_vs")
      ),
      codeSystemFiles = Seq(
        CodeSystemFile("sample-code-system.csv", "sample-code-system.csv", "http://snomed.info/sct")
      )
    )

    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, new MappingContextLoader, schemaRepository, Map.empty, sparkSession)

    val mappingTask = FhirMappingTask("https://aiccelerate.eu/fhir/mappings/specimen-mapping-using-ts", Map("source" -> FileSystemSource("specimen.csv")))

    fhirMappingJobManager.executeMappingTaskAndReturn(mappingJobExecution = FhirMappingJobExecution(job = fhirMappingJob, mappingTasks = Seq(mappingTask)), dataSourceSettings, Some(terminologyServiceSettings)) flatMap { result =>
      val resources = result.map(mappingResult => {
        mappingResult.mappedResource should not be None
        val resource = mappingResult.mappedResource.get.parseJson
        resource shouldBe a[Resource]
        resource
      })
      resources.length shouldBe 2
      FHIRUtil.extractValueOptionByPath[Seq[String]](resources.head, "type.coding.code").getOrElse(Nil).toSet shouldEqual Set("309068002", "111")
      FHIRUtil.extractValueOptionByPath[Seq[String]](resources.head, "type.coding.display").getOrElse(Nil).toSet shouldEqual Set("Specimen from skin", "Eiterprobe")
      FHIRUtil.extractValueOptionByPath[Seq[String]](resources.last, "type.coding.code").getOrElse(Nil).toSet shouldEqual Set("111")
    }
  }

  it should "execute the FhirMappingJob using an identity service" in {
    val lMappingJob = FhirMappingJobFormatter.readMappingJobFromFile(testMappingJobWithIdentityServiceFilePath)

    val terminologyServiceFolderPath = Paths.get(getClass.getResource("/terminology-service").toURI).normalize().toAbsolutePath.toString
    val terminologyServiceSettings = LocalFhirTerminologyServiceSettings(terminologyServiceFolderPath)

    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, new MappingContextLoader, schemaRepository, Map(FhirPathIdentityServiceFunctionsFactory.defaultPrefix -> FhirPathIdentityServiceFunctionsFactory,
      FhirPathUtilFunctionsFactory.defaultPrefix -> FhirPathUtilFunctionsFactory), sparkSession)
    fhirMappingJobManager
      .executeMappingJob(
        mappingJobExecution = FhirMappingJobExecution(mappingTasks = lMappingJob.mappings, job = fhirMappingJob),
        sourceSettings = dataSourceSettings,
        sinkSettings = fhirSinkSettings,
        terminologyServiceSettings = Some(terminologyServiceSettings),
        identityServiceSettings = lMappingJob.copy(sinkSettings = lMappingJob.sinkSettings.asInstanceOf[FhirRepositorySinkSettings].copy(fhirRepoUrl = onFhirClient.getBaseUrl())).getIdentityServiceSettings()) map { res =>
      res shouldBe a[Unit]
    }
  }

  it should "execute the FhirMappingJob with preprocess" in {
    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, Map.empty, sparkSession)
    fhirMappingJobManager.executeMappingTaskAndReturn(mappingJobExecution = FhirMappingJobExecution(mappingTasks = Seq(patientMappingTaskWithPreprocess), job = fhirMappingJob), sourceSettings = dataSourceSettings) map { mappingResults =>
      val results = mappingResults.map(r => {
        r.mappedResource should not be None
        val resource = r.mappedResource.get.parseJson
        resource shouldBe a[Resource]
        resource
      })
      results.size shouldBe 10
      val patient1 = results.head
      FHIRUtil.extractResourceType(patient1) shouldBe "Patient"
      FHIRUtil.extractIdFromResource(patient1) shouldBe FhirMappingUtility.getHashedId("Patient", "p1")
      FHIRUtil.extractValue[String](patient1, "gender") shouldBe "male"
    }
  }

}
