package io.onfhir.tofhir.engine

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import io.onfhir.api.util.FHIRUtil
import io.onfhir.client.OnFhirNetworkClient
import io.onfhir.tofhir.ToFhirTestSpec
import io.onfhir.tofhir.config.{MappingErrorHandling, ToFhirConfig}
import io.onfhir.tofhir.model.{FhirMappingTask, FhirRepositorySinkSettings, SqlSource, SqlSourceSettings}
import io.onfhir.tofhir.util.FhirMappingUtility
import io.onfhir.tofhir.utils.H2DatabaseUtil
import io.onfhir.util.JsonFormatter.formats
import org.scalatest.BeforeAndAfterAll

import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

class SqlSourceTest extends ToFhirTestSpec with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    H2DatabaseUtil.runSqlQuery("patients-otherobservations-populate.sql")
  }

  override protected def afterAll(): Unit = {
    H2DatabaseUtil.runSqlQuery("patients-otherobservations-drop.sql")
    super.afterAll()
  }

  val sqlSourceSettings: SqlSourceSettings = SqlSourceSettings(name = "test-db-source", sourceUri = "https://aiccelerate.eu/data-integration-suite/test-data",
    databaseUrl = H2DatabaseUtil.DATABASE_URL, username = "", password = "")

  val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, sparkSession, MappingErrorHandling.withName(ToFhirConfig.mappingErrorHandling))

  val fhirSinkSetting: FhirRepositorySinkSettings = FhirRepositorySinkSettings(fhirRepoUrl = "http://localhost:8081/fhir", writeErrorHandling = MappingErrorHandling.CONTINUE)
  implicit val actorSystem: ActorSystem = ActorSystem("SqlSourceTest")
  val onFhirClient: OnFhirNetworkClient = OnFhirNetworkClient.apply(fhirSinkSetting.fhirRepoUrl)

  val fhirServerIsAvailable: Boolean =
    Try(Await.result(onFhirClient.search("Patient").execute(), FiniteDuration(5, TimeUnit.SECONDS)).httpStatus == StatusCodes.OK)
      .getOrElse(false)

  val patientMappingTask: FhirMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/patient-mapping",
    sourceContext = Map("source" -> SqlSource("patients", sqlSourceSettings)))

  val otherObsMappingTask: FhirMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/other-observation-mapping",
    sourceContext = Map("source" -> SqlSource("otherobservations", sqlSourceSettings)))

  "Patient mapping" should "should read data from SQL source and map it" in {
    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, sparkSession, MappingErrorHandling.withName(ToFhirConfig.mappingErrorHandling))
    fhirMappingJobManager.executeMappingTaskAndReturn(task = patientMappingTask) map { results =>
      results.size shouldBe 10
      val patient1 = results.head
      FHIRUtil.extractResourceType(patient1) shouldBe "Patient"
      FHIRUtil.extractIdFromResource(patient1) shouldBe FhirMappingUtility.getHashedId("Patient", "p1")
      FHIRUtil.extractValue[String](patient1, "gender") shouldBe "male"
      FHIRUtil.extractValue[String](patient1, "birthDate") shouldBe "2000-05-10"
    }
  }

  it should "map test data and write it to FHIR repo successfully" in {
    //Send it to our fhir repo if they are also validated
    assume(fhirServerIsAvailable)
    fhirMappingJobManager
      .executeMappingJob(tasks = Seq(patientMappingTask), sinkSettings = fhirSinkSetting)
      .map(unit =>
        unit shouldBe()
      )
  }

  "Other observations mapping" should "should read data from SQL source and map it" in {
    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, sparkSession, MappingErrorHandling.withName(ToFhirConfig.mappingErrorHandling))
    fhirMappingJobManager.executeMappingTaskAndReturn(task = otherObsMappingTask) map { results =>
      results.size shouldBe 14
      val observation = results.head
      FHIRUtil.extractResourceType(observation) shouldBe "Observation"
      (observation \ "encounter" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Encounter", "e1")
      (observation \ "code" \ "coding" \ "code").extract[Seq[String]].head shouldBe "9110-8"
      (observation \ "valueQuantity" \ "value").extract[Int] shouldBe 450
    }
  }

  it should "map test data and write it to FHIR repo successfully" in {
    assume(fhirServerIsAvailable)
    fhirMappingJobManager
      .executeMappingJob(tasks = Seq(otherObsMappingTask), sinkSettings = fhirSinkSetting)
      .map(unit =>
        unit shouldBe()
      )
  }

}

