package io.onfhir.tofhir.engine

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import com.typesafe.scalalogging.Logger
import io.onfhir.api.client.FhirBatchTransactionRequestBuilder
import io.onfhir.api.util.FHIRUtil
import io.onfhir.client.OnFhirNetworkClient
import io.onfhir.tofhir.ToFhirTestSpec
import io.onfhir.tofhir.config.MappingErrorHandling
import io.onfhir.tofhir.model.{FhirMappingTask, FhirRepositorySinkSettings, SqlSource, SqlSourceSettings}
import io.onfhir.tofhir.util.{FhirMappingJobFormatter, FhirMappingUtility}
import io.onfhir.util.JsonFormatter.formats
import org.json4s.JsonAST.JObject
import org.scalatest.BeforeAndAfterAll

import java.sql.{Connection, DriverManager, Statement}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, Future}
import scala.io.{BufferedSource, Source}
import scala.util.{Failure, Success, Try, Using}

class SqlSourceTest extends ToFhirTestSpec with BeforeAndAfterAll {

  val logger: Logger = Logger(this.getClass)

  val DATABASE_URL = "jdbc:h2:mem:inputDb;MODE=PostgreSQL;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=FALSE"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val sql = readFileContent("/sql/sql-source-populate.sql")
    runSQL(sql)
  }

  override protected def afterAll(): Unit = {
    val sql = readFileContent("/sql/sql-source-drop.sql")
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

  val testSqlMappingJobFilePath: String = getClass.getResource("/test-sql-mappingjob.json").toURI.getPath

  val sqlSourceSettings =
    Map(
      "source" ->
        SqlSourceSettings(name = "test-db-source", sourceUri = "https://aiccelerate.eu/data-integration-suite/test-data", databaseUrl = DATABASE_URL, username = "", password = "")
    )

  val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, sparkSession, mappingErrorHandling)

  val fhirSinkSetting: FhirRepositorySinkSettings = FhirRepositorySinkSettings(fhirRepoUrl = "http://localhost:8081/fhir", writeErrorHandling = MappingErrorHandling.HALT)
  implicit val actorSystem: ActorSystem = ActorSystem("SqlSourceTest")
  val onFhirClient: OnFhirNetworkClient = OnFhirNetworkClient.apply(fhirSinkSetting.fhirRepoUrl)

  val fhirServerIsAvailable: Boolean =
    Try(Await.result(onFhirClient.search("Patient").execute(), FiniteDuration(5, TimeUnit.SECONDS)).httpStatus == StatusCodes.OK)
      .getOrElse(false)

  // sql tablename mappings tasks
  val patientMappingTask: FhirMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/patient-sql-mapping",
    sourceContext = Map("source" -> SqlSource(tableName = Some("patients"))))

  val otherObsMappingTask: FhirMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/other-observation-sql-mapping",
    sourceContext = Map("source" -> SqlSource(tableName = Some("otherobservations"))))

  // sql query mappings tasks
  val careSiteMappingTask: FhirMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/care-site-sql-mapping",
    sourceContext = Map("source" -> SqlSource(
      query = Some("select cs.care_site_id, cs.care_site_name, c.concept_code, c.vocabulary_id, c.concept_name, l.address_1, l.address_2, l.city, l.state, l.zip " +
        "from care_site cs, location l, concept c " +
        "where cs.location_id = l.location_id and cs.place_of_service_concept_id = c.concept_id"))))

  val locationMappingTask: FhirMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/location-sql-mapping",
    sourceContext = Map("source" -> SqlSource(
      query = Some("select * from location"))))

  val procedureOccurrenceMappingTask: FhirMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/procedure-occurrence-sql-mapping",
    sourceContext = Map("source" -> SqlSource(
      query = Some("select po.procedure_occurrence_id, po.visit_occurrence_id, po.person_id, c.concept_code, c.vocabulary_id, c.concept_name, " +
        "po.procedure_date, po.procedure_datetime, po.provider_id " +
        "from procedure_occurrence po left join concept c on po.procedure_concept_id = c.concept_id"))))

  "Patient mapping" should "should read data from SQL source and map it" in {
    fhirMappingJobManager.executeMappingTaskAndReturn(task = patientMappingTask, sourceSettings = sqlSourceSettings) map { results =>
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
      .executeMappingJob(tasks = Seq(patientMappingTask), sourceSettings = sqlSourceSettings, sinkSettings = fhirSinkSetting)
      .flatMap(_ => {
        //Delete patients
        var batchRequest: FhirBatchTransactionRequestBuilder = onFhirClient.batch()
        (1 to 10).foreach { i =>
          batchRequest = batchRequest.entry(_.delete("Patient", FhirMappingUtility.getHashedId("Patient", "p" + i.toString )))
        }
        batchRequest.returnMinimal().asInstanceOf[FhirBatchTransactionRequestBuilder].execute() map { res =>
          res.httpStatus shouldBe StatusCodes.OK
        }
      })
  }

  "Other observations mapping" should "should read data from SQL source and map it" in {
    fhirMappingJobManager.executeMappingTaskAndReturn(task = otherObsMappingTask, sourceSettings = sqlSourceSettings) map { results =>
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
      .executeMappingJob(tasks = Seq(otherObsMappingTask), sourceSettings = sqlSourceSettings, sinkSettings = fhirSinkSetting)
      .flatMap(_ => {
        // Delete all observations
        var batchRequest: FhirBatchTransactionRequestBuilder = onFhirClient.batch()
        val obsSearchFutures = (1 to 10).map(i => {
          onFhirClient.search("Observation").where("subject", "Patient/" + FhirMappingUtility.getHashedId("Patient", "p"+i))
            .executeAndReturnBundle()
        })
        Future.sequence(obsSearchFutures) flatMap { obsBundleList =>
          obsBundleList.foreach(observationBundle => {
            observationBundle.searchResults.foreach(obs =>
              batchRequest = batchRequest.entry(_.delete("Observation", (obs \ "id").extract[String]))
            )
          })
          batchRequest.returnMinimal().asInstanceOf[FhirBatchTransactionRequestBuilder].execute() map { res =>
            res.httpStatus shouldBe StatusCodes.OK
          }
        }
      })
  }

  "Care site mapping" should "should read data from SQL source and map it" in {
    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, sparkSession, mappingErrorHandling)
    fhirMappingJobManager.executeMappingTaskAndReturn(task = careSiteMappingTask, sourceSettings = sqlSourceSettings) map { results =>
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
      .executeMappingJob(tasks = Seq(careSiteMappingTask), sourceSettings = sqlSourceSettings, sinkSettings = fhirSinkSetting)
      .flatMap(_ => {
        //Delete care sites
        var batchRequest: FhirBatchTransactionRequestBuilder = onFhirClient.batch()
        (1 to 2).foreach { i =>
          batchRequest = batchRequest.entry(_.delete("Organization", FhirMappingUtility.getHashedId("Organization", i.toString )))
        }
        batchRequest.returnMinimal().asInstanceOf[FhirBatchTransactionRequestBuilder].execute() map { res =>
          res.httpStatus shouldBe StatusCodes.OK
        }
      })
  }

  "Location mapping" should "should read data from SQL source and map it" in {
    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, sparkSession, mappingErrorHandling)
    fhirMappingJobManager.executeMappingTaskAndReturn(task = locationMappingTask, sourceSettings = sqlSourceSettings) map { results =>
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
      .executeMappingJob(tasks = Seq(locationMappingTask), sourceSettings = sqlSourceSettings, sinkSettings = fhirSinkSetting)
      .flatMap(_ => {
        //Delete locations
        var batchRequest: FhirBatchTransactionRequestBuilder = onFhirClient.batch()
        (1 to 5).foreach { i =>
          batchRequest = batchRequest.entry(_.delete("Location", FhirMappingUtility.getHashedId("Location", i.toString )))
        }
        batchRequest.returnMinimal().asInstanceOf[FhirBatchTransactionRequestBuilder].execute() map { res =>
          res.httpStatus shouldBe StatusCodes.OK
        }
      })
  }

  "Procedure occurrence mapping" should "should read data from SQL source and map it" in {
    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, sparkSession, mappingErrorHandling)
    fhirMappingJobManager.executeMappingTaskAndReturn(task = procedureOccurrenceMappingTask, sourceSettings = sqlSourceSettings) map { results =>
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
      .executeMappingJob(tasks = Seq(procedureOccurrenceMappingTask), sourceSettings = sqlSourceSettings, sinkSettings = fhirSinkSetting)
      .flatMap(_ => {
        //Delete procedures
        var batchRequest: FhirBatchTransactionRequestBuilder = onFhirClient.batch()
        (1 to 5).foreach { i =>
          batchRequest = batchRequest.entry(_.delete("Procedure", FhirMappingUtility.getHashedId("Procedure", i.toString)))
        }
        batchRequest.returnMinimal().asInstanceOf[FhirBatchTransactionRequestBuilder].execute() map { res =>
          res.httpStatus shouldBe StatusCodes.OK
        }
      })
  }

  it should "execute the FhirMappingJob with SQL source and sink settings restored from a file" in {
    assume(fhirServerIsAvailable)
    val lMappingJob = FhirMappingJobFormatter.readMappingJobFromFile(testSqlMappingJobFilePath)


    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, new MappingContextLoader(mappingRepository), schemaRepository, sparkSession, lMappingJob.mappingErrorHandling)
    fhirMappingJobManager.executeMappingJob(tasks = lMappingJob.mappings, sourceSettings = lMappingJob.sourceSettings, sinkSettings = lMappingJob.sinkSettings) flatMap { unit =>
      //Delete written resources
      var batchRequest: FhirBatchTransactionRequestBuilder = onFhirClient.batch()
      (1 to 10).foreach { i =>
        batchRequest = batchRequest.entry(_.delete("Patient", FhirMappingUtility.getHashedId("Patient", "p" + i.toString )))
      }
      (1 to 2).foreach { i =>
        batchRequest = batchRequest.entry(_.delete("Organization", FhirMappingUtility.getHashedId("Organization", i.toString )))
      }
      (1 to 5).foreach { i =>
        batchRequest = batchRequest.entry(_.delete("Location", FhirMappingUtility.getHashedId("Location", i.toString )))
      }
      batchRequest.returnMinimal().asInstanceOf[FhirBatchTransactionRequestBuilder].execute() map { res =>
        res.httpStatus shouldBe StatusCodes.OK
      }
    }
  }

}

