package io.onfhir.tofhir.engine

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import com.typesafe.scalalogging.Logger
import io.onfhir.api.util.FHIRUtil
import io.onfhir.client.OnFhirNetworkClient
import io.onfhir.tofhir.ToFhirTestSpec
import io.onfhir.tofhir.config.{MappingErrorHandling, ToFhirConfig}
import io.onfhir.tofhir.model.{FhirMappingTask, FhirRepositorySinkSettings, SqlSource, SqlSourceSettings}
import io.onfhir.tofhir.util.FhirMappingUtility
import io.onfhir.util.JsonFormatter.formats
import org.scalatest.BeforeAndAfterAll

import java.io.FileNotFoundException
import java.nio.charset.StandardCharsets
import java.sql.{Connection, DriverManager, Statement}
import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration
import scala.io.{BufferedSource, Source}
import scala.util.{Failure, Success, Try, Using}

class SqlSourceTest extends ToFhirTestSpec with BeforeAndAfterAll {

  val logger: Logger = Logger(this.getClass)

  val DATABASE_URL = "jdbc:h2:mem:inputDb;MODE=PostgreSQL;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=FALSE"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val sql = readFileContent("/sql/patients-otherobservations-populate.sql")
    runSQL(sql)
  }

  override protected def afterAll(): Unit = {
    val sql = readFileContent("/sql/patients-otherobservations-drop.sql")
    runSQL(sql)
    super.afterAll()
  }

  private def readFileContent(fileName: String): String = {
    val source: BufferedSource = Source.fromInputStream(getClass.getResourceAsStream(fileName))
    try source.mkString finally source.close()
  }

  private def runSQL(sql: String): Boolean = {
    Using.Manager { use =>
      val con: Connection = use(DriverManager.getConnection(DATABASE_URL))
      val stm: Statement = use(con.createStatement)
      stm.execute(sql)
    } match {
      case Success(value) => value
      case Failure(e)     => throw e
    }
  }

  val sqlSourceSettings: SqlSourceSettings = SqlSourceSettings(name = "test-db-source", sourceUri = "https://aiccelerate.eu/data-integration-suite/test-data",
    databaseUrl = DATABASE_URL, username = "", password = "")

  val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, sparkSession, mappingErrorHandling)

  val fhirSinkSetting: FhirRepositorySinkSettings = FhirRepositorySinkSettings(fhirRepoUrl = "http://localhost:8081/fhir", writeErrorHandling = MappingErrorHandling.HALT)
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

