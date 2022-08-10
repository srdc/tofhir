package io.onfhir.tofhir.engine

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import com.typesafe.scalalogging.Logger
import io.onfhir.api.client.FhirBatchTransactionRequestBuilder
import io.onfhir.api.util.FHIRUtil
import io.onfhir.client.OnFhirNetworkClient
import io.onfhir.tofhir.ToFhirTestSpec
import io.onfhir.tofhir.config.{MappingErrorHandling, ToFhirConfig}
import io.onfhir.tofhir.model._
import io.onfhir.tofhir.util.{FhirMappingJobFormatter, FhirMappingUtility}
import io.onfhir.util.JsonFormatter.formats
import it.sauronsoftware.cron4j.Scheduler
import org.apache.commons.io.FileUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{Assertion, BeforeAndAfterAll}

import java.io.File
import java.net.URI
import java.nio.file.{Path, Paths}
import java.sql.{Connection, DriverManager, Statement}
import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{Await, Future}
import scala.io.{BufferedSource, Source}
import scala.util.{Failure, Success, Try, Using}

class SchedulingTest extends AnyFlatSpec with BeforeAndAfterAll with ToFhirTestSpec {

  val logger: Logger = Logger(this.getClass)

  val DATABASE_URL = "jdbc:h2:mem:inputDb;MODE=PostgreSQL;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=FALSE"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val sql = readFileContent("/sql/scheduling-populate.sql")
    runSQL(sql)
  }

  override protected def afterAll(): Unit = {
    val sql = readFileContent("/sql/scheduling-drop.sql")
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
      case Failure(e) => throw e
    }
  }

  private def deleteResources(): Future[Assertion] = {
    var batchRequest: FhirBatchTransactionRequestBuilder = onFhirClient.batch()
    // Delete all patients between p1-p10 and related observation
    (1 to 10).foreach(i => {
      batchRequest = batchRequest.entry(_.delete("Patient", FhirMappingUtility.getHashedId("Patient", "p" + i)))
    })
    onFhirClient.search("Observation").where("subject", "Patient/" + FhirMappingUtility.getHashedId("Patient", "p4")) flatMap { observationBundle =>
      observationBundle.searchResults.foreach(obs => {
        batchRequest = batchRequest.entry(_.delete("Observation", (obs \ "id").extract[String]))
      })
      batchRequest.returnMinimal().asInstanceOf[FhirBatchTransactionRequestBuilder].execute() map { res =>
        res.httpStatus shouldBe StatusCodes.OK
      }
    }
  }

  val scheduler = new Scheduler()

  val resourcePath: URI = getClass.getResource("/").toURI
  val toFhirDb: Path = Paths.get(resourcePath.resolve(ToFhirConfig.toFhirDb.get))

  val mappingJobScheduler: MappingJobScheduler = MappingJobScheduler(scheduler, toFhirDb.toUri)

  val fhirSinkSettings: FhirRepositorySinkSettings = FhirRepositorySinkSettings(fhirRepoUrl = "http://localhost:8081/fhir", writeErrorHandling = MappingErrorHandling.CONTINUE)

  implicit val actorSystem: ActorSystem = ActorSystem("SchedulingTest")
  val onFhirClient: OnFhirNetworkClient = OnFhirNetworkClient.apply(fhirSinkSettings.fhirRepoUrl)
  val fhirServerIsAvailable: Boolean =
    Try(Await.result(onFhirClient.search("Patient").execute(), FiniteDuration(5, TimeUnit.SECONDS)).httpStatus == StatusCodes.OK)
      .getOrElse(false)

  val testScheduleMappingJobFilePath: String = getClass.getResource("/test-schedule-mappingjob.json").toURI.getPath

  it should "schedule a FhirMappingJob with cron and sink settings restored from a file" in {
    assume(fhirServerIsAvailable)
    val lMappingJob: FhirMappingJob = FhirMappingJobFormatter.readMappingJobFromFile(testScheduleMappingJobFilePath)

    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, new MappingContextLoader(mappingRepository), schemaRepository, sparkSession, lMappingJob.mappingErrorHandling, Some(mappingJobScheduler))
    fhirMappingJobManager.scheduleMappingJob(tasks = lMappingJob.mappings, sourceSettings = lMappingJob.sourceSettings, sinkSettings = lMappingJob.sinkSettings, schedulingSettings = lMappingJob.schedulingSettings.get)
    scheduler.start() //job set to run every minute
    Thread.sleep(60000) //wait for the job to be executed once

    val directory = new File(toFhirDb.toUri)
    FileUtils.cleanDirectory(directory)

    val searchTest = onFhirClient.read("Patient", FhirMappingUtility.getHashedId("Patient", "p8")).executeAndReturnResource() flatMap { p1Resource =>
      FHIRUtil.extractIdFromResource(p1Resource) shouldBe FhirMappingUtility.getHashedId("Patient", "p8")
      FHIRUtil.extractValue[String](p1Resource, "gender") shouldBe "female"
      FHIRUtil.extractValue[String](p1Resource, "birthDate") shouldBe "2010-01-10"

      onFhirClient.search("Observation").where("code", "9269-2").executeAndReturnBundle() flatMap { observationBundle =>
        //the Observation with the code 9269-2 matches our time range, others should not
        observationBundle.searchResults.length shouldBe 1
        (observationBundle.searchResults.head \ "subject" \ "reference").extract[String] shouldBe
          FhirMappingUtility.getHashedReference("Patient", "p4")
        //the Observation with the code 445619006, as an example, does not match our time range
        onFhirClient.search("Observation").where("code", "445619006").executeAndReturnBundle() flatMap { emptyObservationBundle =>
          emptyObservationBundle.searchResults shouldBe empty
          onFhirClient.search("MedicationAdministration").where("code", "313002").executeAndReturnBundle() map { medicationAdministrationBundle =>
            medicationAdministrationBundle.searchResults shouldBe empty
            deleteResources()
          }
        }
      }
    }
    Await.result(searchTest, Duration.Inf)
  }


}

