package io.onfhir.tofhir.engine

import io.onfhir.tofhir.ToFhirTestSpec
import io.onfhir.tofhir.model.{FhirMappingFromFileSystemTask, FileSystemSourceSettings, SourceFileFormats}
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

  "A FhirMappingJobManager" should "execute the patient mapping task" in {

    val patientMappingTask = FhirMappingFromFileSystemTask(
      mappingRef = "https://aiccelerate.eu/fhir/mappings/patient-mapping",
      path = "patients.csv",
      sourceType = SourceFileFormats.CSV)

    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, sparkSession)
    fhirMappingJobManager.executeMappingTaskAndReturn(sourceSettings = dataSourceSettings, task = patientMappingTask) map { results =>
      results.size shouldBe 10
    }
  }

}
