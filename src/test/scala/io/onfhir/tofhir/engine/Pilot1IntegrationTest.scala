package io.onfhir.tofhir.engine

import io.onfhir.tofhir.ToFhirTestSpec
import io.onfhir.tofhir.model.{FhirMappingFromFileSystemTask, FileSystemSourceSettings, SourceFileFormats}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.net.URI

class Pilot1IntegrationTest extends ToFhirTestSpec {

  val sparkConf:SparkConf = new SparkConf()
    .setAppName("tofhir-test")
    .setMaster("local")
    .set("spark.driver.allowMultipleContexts", "false")
    .set("spark.ui.enabled", "false")

  val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  val mappingRepository: IFhirMappingRepository =
    new FhirMappingFolderRepository(new URI("file:///D:/codes/aiccelerate/data-integration-suite/mappings/pilot1"))

  val contextLoader: IMappingContextLoader = new MappingContextLoader(mappingRepository)

  val dataSourceSettings: FileSystemSourceSettings = FileSystemSourceSettings("test-source-1", "http://hus.fi", new URI("file:///D:/codes/aiccelerate/data-integration-suite/test-data/pilot1"))

  val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, sparkSession)

  "patient mapping" should "map test data" in {
    val patientMappingTask =
      FhirMappingFromFileSystemTask(
        mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot1/patient-mapping",
        path = "patients.csv",
        sourceType = SourceFileFormats.CSV
      )

    fhirMappingJobManager.executeMappingTaskAndReturn(sourceSettings = dataSourceSettings, task = patientMappingTask) map { results =>
      results.size shouldBe 10
    }
  }

}
