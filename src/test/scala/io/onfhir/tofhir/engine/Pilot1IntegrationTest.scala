package io.onfhir.tofhir.engine

import io.onfhir.tofhir.ToFhirTestSpec
import io.onfhir.tofhir.model.{FhirMappingTask, FileSystemSource, FileSystemSourceSettings, SourceFileFormats}
import io.onfhir.util.JsonFormatter.formats
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.json4s.JArray

import java.net.URI
import java.nio.file.Paths

class Pilot1IntegrationTest extends ToFhirTestSpec {

  val sparkConf:SparkConf = new SparkConf()
    .setAppName("tofhir-test")
    .setMaster("local")
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

  "patient mapping" should "map test data" in {
    val patientMappingTask = FhirMappingTask(
      mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot1/patient-mapping",
      sourceContext = Map("source" ->  FileSystemSource(
        path = "patients.csv",
        sourceType = SourceFileFormats.CSV,
        dataSourceSettings
      ))
    )


    fhirMappingJobManager.executeMappingTaskAndReturn(task = patientMappingTask) map { results =>
      val genders = (JArray(results.toList) \ "gender").extract[Seq[String]]
      genders shouldBe ((1 to 5).map(_ => "male") ++ (6 to 10).map(_=> "female"))

      (results.apply(2) \ "id").extract[String] shouldBe("p3")
      (results.apply(2) \ "birthDate").extract[String] shouldBe("1997-02")
      (results.apply(3) \ "address" \ "postalCode").extract[Seq[String]] shouldBe(Seq("H10564"))
      (results.apply(4) \ "deceasedDateTime" ).extract[String] shouldBe("2019-04-21")
    }
  }

  "operation episode encounter mapping" should "map test data" in {
    val mappingTask =
      FhirMappingTask(
        mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot1/operation-episode-encounter-mapping",
        sourceContext = Map("source" ->  FileSystemSource(
          path = "operation-episode-encounters.csv",
          sourceType = SourceFileFormats.CSV,
          dataSourceSettings
        ))
      )


    fhirMappingJobManager.executeMappingTaskAndReturn(task = mappingTask) map { results =>
      results.size shouldBe(10)
      (results.apply(1) \ "id").extract[String] shouldBe "e2"
      (results.apply(1) \ "subject" \ "reference").extract[String] shouldBe("Patient/p2")
      (results.apply(3) \ "episodeOfCare" \ "reference").extract[Seq[String]] shouldBe Seq("EpisodeOfCare/ep4")
      (results.apply(9) \ "status").extract[String] shouldBe "cancelled"
      (results.apply(5) \ "type" \ "coding" \ "code").extract[Seq[String]] shouldBe Seq("305354007")
      (results.apply(5) \ "type" \ "coding" \ "display").extract[Seq[String]] shouldBe Seq("Ward stay")
      (results.apply(6) \ "period" \ "start").extract[String] shouldBe "2014-10-20"
      (results.apply(3) \ "period" \ "end").extractOpt[String] shouldBe empty
      (results.apply(7) \ "location" \ "location" \ "reference").extract[Seq[String]] shouldBe Seq("Location/ward2")
      (results.apply(0) \ "participant" \ "individual" \ "reference").extract[Seq[String]] shouldBe Seq("PractitionerRole/pr-pr4")
    }
  }

  "surgery plan mapping" should "map test data" in {
    val mappingTask = FhirMappingTask(
        mappingRef = "https://aiccelerate.eu/fhir/mappings/pilot1/surgery-plan-mapping",
        sourceContext = Map("source" ->FileSystemSource(
          path = "surgery-plans.csv",
          sourceType = SourceFileFormats.CSV,
          dataSourceSettings
        ))
      )

    fhirMappingJobManager.executeMappingTaskAndReturn(task = mappingTask) map { results =>
      results.size shouldBe 4
    }
  }



}
