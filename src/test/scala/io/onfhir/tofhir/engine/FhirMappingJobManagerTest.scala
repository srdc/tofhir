package io.onfhir.tofhir.engine

import io.onfhir.api.util.FHIRUtil
import io.onfhir.tofhir.ToFhirTestSpec
import io.onfhir.tofhir.model.{FhirMappingFromFileSystemTask, FhirRepositorySinkSettings, FhirSinkSettings, FileSystemSourceSettings, SourceFileFormats}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class FhirMappingJobManagerTest extends ToFhirTestSpec {

  val sparkConf:SparkConf = new SparkConf()
    .setAppName("tofhir-test")
    .setMaster("local")
    .set("spark.driver.allowMultipleContexts", "false")
    .set("spark.ui.enabled", "false")
  val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  val mappingRepository: IFhirMappingRepository =
    new FhirMappingFolderRepository(getClass.getResource("/test-mappings-1").toURI)
  val contextLoader: IMappingContextLoader = new MappingContextLoader(mappingRepository)

  val dataSourceSettings: FileSystemSourceSettings = FileSystemSourceSettings("test-source-1", "file:///test-source-1", getClass.getResource("/test-data-1").toURI)
  val fhirSinkSetting: FhirRepositorySinkSettings = FhirRepositorySinkSettings("http://localhost:8081/fhir")

  val patientMappingTask = FhirMappingFromFileSystemTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/patient-mapping",
    path = "patients.csv",
    sourceType = SourceFileFormats.CSV)

//  "A FhirMappingJobManager" should "execute the patient mapping task and return the results" in {
//    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, sparkSession)
//    fhirMappingJobManager.executeMappingTaskAndReturn(sourceSettings = dataSourceSettings, task = patientMappingTask) map { results =>
//      results.size shouldBe 10
//      val patient1 = results.head
//      FHIRUtil.extractResourceType(patient1) shouldBe "Patient"
//      FHIRUtil.extractIdFromResource(patient1) shouldBe "p1"
//      FHIRUtil.extractValue[String](patient1, "gender") shouldBe "male"
//
//      val patient2 = results(1)
//      FHIRUtil.extractResourceType(patient2) shouldBe "Patient"
//      FHIRUtil.extractIdFromResource(patient2) shouldBe "p2"
//      FHIRUtil.extractValue[String](patient2, "deceasedDateTime") shouldBe "2017-03-10"
//
//      val patient10 = results.last
//      FHIRUtil.extractResourceType(patient10) shouldBe "Patient"
//      FHIRUtil.extractIdFromResource(patient10) shouldBe "p10"
//      FHIRUtil.extractValue[String](patient10, "gender") shouldBe "female"
//      FHIRUtil.extractValue[String](patient10, "birthDate") shouldBe "2003-11"
//      FHIRUtil.extractValueOption[String](patient10, "deceasedDateTime").isEmpty shouldBe true
//    }
//  }

  it should "execute the a mapping job with a single task and write the results into a FHIR repository" in {
    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, sparkSession)
    fhirMappingJobManager.executeMappingJob(sourceSettings = dataSourceSettings, tasks = Seq(patientMappingTask), sinkSettings = fhirSinkSetting) map { res =>
      res.isInstanceOf[Unit] shouldBe true
    }
  }

}
