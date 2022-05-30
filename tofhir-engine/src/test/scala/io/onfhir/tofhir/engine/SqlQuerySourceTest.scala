package io.onfhir.tofhir.engine

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import com.typesafe.scalalogging.Logger
import io.onfhir.api.util.FHIRUtil
import io.onfhir.client.OnFhirNetworkClient
import io.onfhir.tofhir.ToFhirTestSpec
import io.onfhir.tofhir.config.{MappingErrorHandling, ToFhirConfig}
import io.onfhir.tofhir.model.{FhirMappingTask, FhirRepositorySinkSettings, SqlQuerySource, SqlSourceSettings}
import io.onfhir.tofhir.util.FhirMappingUtility
import io.onfhir.util.JsonFormatter.formats
import org.json4s.JsonAST.JObject
import org.scalatest.BeforeAndAfterAll

import java.sql.{Connection, DriverManager, Statement}
import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration
import scala.io.{BufferedSource, Source}
import scala.util.{Failure, Success, Try, Using}

class SqlQuerySourceTest extends ToFhirTestSpec with BeforeAndAfterAll  {

  val logger: Logger = Logger(this.getClass)

  val DATABASE_URL = "jdbc:h2:mem:inputDb;MODE=PostgreSQL;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=FALSE"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val sql = readFileContent("/sql/omop-sql-query-populate.sql")
    runSQL(sql)
  }

  override protected def afterAll(): Unit = {
    val sql = readFileContent("/sql/omop-sql-query-drop.sql")
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

  val fhirSinkSetting: FhirRepositorySinkSettings = FhirRepositorySinkSettings(fhirRepoUrl = "http://localhost:8081/fhir", writeErrorHandling = MappingErrorHandling.CONTINUE)
  implicit val actorSystem: ActorSystem = ActorSystem("SqlSourceQueryTest")
  val onFhirClient: OnFhirNetworkClient = OnFhirNetworkClient.apply(fhirSinkSetting.fhirRepoUrl)

  val fhirServerIsAvailable: Boolean =
    Try(Await.result(onFhirClient.search("Patient").execute(), FiniteDuration(5, TimeUnit.SECONDS)).httpStatus == StatusCodes.OK)
      .getOrElse(false)

  val careSiteMappingTask: FhirMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/omop/care-site-mapping",
    sourceContext = Map("source" -> SqlQuerySource(
      "select cs.care_site_id, cs.care_site_name, c.concept_code, c.vocabulary_id, c.concept_name, l.address_1, l.address_2, l.city, l.state, l.zip " +
        "from care_site cs, location l, concept c " +
        "where cs.location_id = l.location_id and cs.place_of_service_concept_id = c.concept_id", sqlSourceSettings)))

  val locationMappingTask: FhirMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/omop/location-mapping",
    sourceContext = Map("source" -> SqlQuerySource(
      "select * from location", sqlSourceSettings)))

  val procedureOccurrenceMappingTask: FhirMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/omop/procedure-occurrence-mapping",
    sourceContext = Map("source" -> SqlQuerySource(
      "select po.procedure_occurrence_id, po.visit_occurrence_id, po.person_id, c.concept_code, c.vocabulary_id, c.concept_name, " +
        "po.procedure_date, po.procedure_datetime, po.provider_id " +
        "from procedure_occurrence po left join concept c on po.procedure_concept_id = c.concept_id", sqlSourceSettings)))

    "Care site mapping" should "should read data from SQL source and map it" in {
      val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, sparkSession, mappingErrorHandling)
      fhirMappingJobManager.executeMappingTaskAndReturn(task = careSiteMappingTask) map { results =>
        results.size shouldBe 2
        val organization1 = results.head
        FHIRUtil.extractResourceType(organization1) shouldBe "Organization"
        (organization1 \ "name").extract[String] shouldBe "Example care site name"
        (((organization1 \ "type").extract[Seq[JObject]].head \ "coding").extract[Seq[JObject]].head \ "code").extract[String] shouldBe "21"
        ((organization1 \ "address").extract[Seq[JObject]].head \ "state").extract[String] shouldBe "MO"
      }
    }

    it should "map test data and write it to FHIR repo successfully" in {
      //Send it to our fhir repo if they are also validated
      assume(fhirServerIsAvailable)
      fhirMappingJobManager
        .executeMappingJob(tasks = Seq(careSiteMappingTask), sinkSettings = fhirSinkSetting)
        .map(unit =>
          unit shouldBe()
        )
    }

    "Location mapping" should "should read data from SQL source and map it" in {
      val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, sparkSession, mappingErrorHandling)
      fhirMappingJobManager.executeMappingTaskAndReturn(task = locationMappingTask) map { results =>
        results.size shouldBe 5
        val location = results.head
        FHIRUtil.extractResourceType(location) shouldBe "Location"
        ((location \ "address").extract[JObject] \ "line").extract[Seq[String]].head shouldBe "19 Farragut"
        ((location \ "address").extract[JObject] \ "state").extract[String] shouldBe "MO"
      }
    }

    it should "map test data and write it to FHIR repo successfully" in {
      assume(fhirServerIsAvailable)
      fhirMappingJobManager
        .executeMappingJob(tasks = Seq(locationMappingTask), sinkSettings = fhirSinkSetting)
        .map(unit =>
          unit shouldBe()
        )
    }

    "Procedure occurrence mapping" should "should read data from SQL source and map it" in {
      val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, sparkSession, mappingErrorHandling)
      fhirMappingJobManager.executeMappingTaskAndReturn(task = procedureOccurrenceMappingTask) map { results =>
        results.size shouldBe 5
        val procedureOccurrence = results.head
        FHIRUtil.extractResourceType(procedureOccurrence) shouldBe "Procedure"
        (procedureOccurrence \ "subject" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Patient", "906440")
        (procedureOccurrence \ "encounter" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Encounter", "43483680")
        ((procedureOccurrence \ "performer").extract[Seq[JObject]].head \ "actor" \ "reference").extract[String] shouldBe FhirMappingUtility.getHashedReference("Practitioner", "48878")
        (procedureOccurrence \ "performedDateTime").extract[String] shouldBe "2010-04-25"
      }
    }

    it should "map test data and write it to FHIR repo successfully" in {
      assume(fhirServerIsAvailable)
      fhirMappingJobManager
        .executeMappingJob(tasks = Seq(procedureOccurrenceMappingTask), sinkSettings = fhirSinkSetting)
        .map(unit =>
          unit shouldBe()
        )
    }

}

