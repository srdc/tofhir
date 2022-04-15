package io.onfhir.tofhir.engine

import akka.http.scaladsl.model.StatusCodes
import io.onfhir.api.Resource
import io.onfhir.api.util.FHIRUtil
import io.onfhir.client.OnFhirNetworkClient
import io.onfhir.tofhir.ToFhirTestSpec
import io.onfhir.tofhir.model.{FhirMappingTask, FhirRepositorySinkSettings, FhirSinkSettings, FileSystemSource, FileSystemSourceSettings, SourceFileFormats}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

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

  val mappingRepository: IFhirMappingRepository =
    new FhirMappingFolderRepository(getClass.getResource("/test-mappings-1").toURI)
  val contextLoader: IMappingContextLoader = new MappingContextLoader(mappingRepository)

  val schemaRepository = new SchemaFolderRepository(getClass.getResource("/test-schema").toURI)

  val dataSourceSettings: FileSystemSourceSettings = FileSystemSourceSettings("test-source-1", "file:///test-source-1", getClass.getResource("/test-data-1").toURI)
  val fhirSinkSetting: FhirRepositorySinkSettings = FhirRepositorySinkSettings("http://localhost:8081/fhir")
  val onFhirClient = OnFhirNetworkClient.apply(fhirSinkSetting.fhirRepoUrl)
  val fhirServerIsAvailable =
    Try(Await.result(onFhirClient.search("Patient").execute(), FiniteDuration(5, TimeUnit.SECONDS)).httpStatus == StatusCodes.OK)
      .getOrElse(false)

  val patientMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/patient-mapping",
    sourceContext = Map(
      "source" ->  FileSystemSource(path = "patients.csv", sourceType = SourceFileFormats.CSV, dataSourceSettings)
    )
  )


  "A FhirMappingJobManager" should "execute the patient mapping task and return the results" in {
    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, sparkSession)
    fhirMappingJobManager.executeMappingTaskAndReturn(task = patientMappingTask) map { results =>
      results.size shouldBe 10
      val patient1 = results.head
      FHIRUtil.extractResourceType(patient1) shouldBe "Patient"
      FHIRUtil.extractIdFromResource(patient1) shouldBe "p1"
      FHIRUtil.extractValue[String](patient1, "gender") shouldBe "male"

      val patient2 = results(1)
      FHIRUtil.extractResourceType(patient2) shouldBe "Patient"
      FHIRUtil.extractIdFromResource(patient2) shouldBe "p2"
      FHIRUtil.extractValue[String](patient2, "deceasedDateTime") shouldBe "2017-03-10"

      val patient10 = results.last
      FHIRUtil.extractResourceType(patient10) shouldBe "Patient"
      FHIRUtil.extractIdFromResource(patient10) shouldBe "p10"
      FHIRUtil.extractValue[String](patient10, "gender") shouldBe "female"
      FHIRUtil.extractValue[String](patient10, "birthDate") shouldBe "2003-11"
      FHIRUtil.extractValueOption[String](patient10, "deceasedDateTime").isEmpty shouldBe true
    }
  }

  it should "execute the a mapping job with a single Patient mapping task and write the results into a FHIR repository" in {
    assume(fhirServerIsAvailable)

    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, sparkSession)
    fhirMappingJobManager.executeMappingJob(tasks = Seq(patientMappingTask), sinkSettings = fhirSinkSetting) flatMap { response =>
      onFhirClient.read("Patient", "p1").executeAndReturnResource() map { p1Resource =>
        FHIRUtil.extractIdFromResource(p1Resource) shouldBe "p1"
      }
    }
  }

}