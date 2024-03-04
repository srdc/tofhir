package io.tofhir.test

import akka.http.scaladsl.model.StatusCodes
import io.onfhir.api.Resource
import io.onfhir.api.client.FhirBatchTransactionRequestBuilder
import io.onfhir.api.util.FHIRUtil
import io.onfhir.client.OnFhirNetworkClient
import io.onfhir.path.FhirPathUtilFunctionsFactory
import io.onfhir.util.JsonFormatter._
import io.tofhir.ToFhirTestSpec
import io.tofhir.engine.mapping.FhirMappingJobManager
import io.tofhir.engine.model._
import io.tofhir.engine.util.FhirMappingJobFormatter.EnvironmentVariable
import org.json4s.JsonAST.JArray
import org.json4s.jackson.JsonMethods
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AsyncFlatSpec

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext}
import scala.io.Source
import scala.util.Try

/**
 * Test suite for verifying the behavior of FhirServerSource.
 */
class FhirServerSourceTest extends AsyncFlatSpec with BeforeAndAfterAll with ToFhirTestSpec {

  // Define implicit execution context for asynchronous operations
  implicit override val executionContext: ExecutionContext = actorSystem.getDispatcher

  // Sink Settings of mapping job
  val fhirSinkSettings: FhirRepositorySinkSettings = FhirRepositorySinkSettings(fhirRepoUrl = sys.env.getOrElse(EnvironmentVariable.FHIR_REPO_URL.toString, "http://localhost:8081/fhir"))
  // Define OnFhir clients for source and target servers
  val targetOnFhirClient: OnFhirNetworkClient = OnFhirNetworkClient.apply(fhirSinkSettings.fhirRepoUrl)
  val sourceOnFhirClient: OnFhirNetworkClient = OnFhirNetworkClient.apply("http://localhost:6080/fhir")

  // Settings of Fhir Server data source
  val fhirServerSourceSettings: Map[String, FhirServerSourceSettings] =
    Map(
      "source" ->
        FhirServerSourceSettings(name = "test-fhir-server-source", sourceUri = "https://test-data", serverUrl = sourceOnFhirClient.getBaseUrl())
    )

  // FhirMappingJobManager to execute mapping job
  val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, Map(FhirPathUtilFunctionsFactory.defaultPrefix -> FhirPathUtilFunctionsFactory), sparkSession)

  // Whether the source onFHIR server is available
  val sourceServerIsAvailable: Boolean =
    Try(Await.result(sourceOnFhirClient.search("Patient").execute(), FiniteDuration(5, TimeUnit.SECONDS)).httpStatus == StatusCodes.OK)
      .getOrElse(false)
  // Whether the target onFHIR server is available
  val targetServerIsAvailable: Boolean =
    Try(Await.result(targetOnFhirClient.search("Patient").execute(), FiniteDuration(5, TimeUnit.SECONDS)).httpStatus == StatusCodes.OK)
      .getOrElse(false)

  // Observation mapping task
  val observationMappingTask: FhirMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/test-observation-mapping",
    sourceContext = Map("source" -> FhirServerSource(resourceType = "Observation")))
  // Mapping Job
  val fhirMappingJob: FhirMappingJob = FhirMappingJob(
    name = Some("test-fhir-server-mappingjob"),
    mappings = Seq.empty,
    sourceSettings = fhirServerSourceSettings,
    sinkSettings = fhirSinkSettings,
    dataProcessingSettings = DataProcessingSettings()
  )

  // Observation resource to be created on the source onFhir server
  val testObservationResource: Resource = JsonMethods.parse(Source.fromInputStream(getClass.getResourceAsStream("/fhir-resources/observation-resource.json")).mkString).extract[Resource]

  /**
   * Create "example-observation" resource on the source onFHIR server before starting the tests.
   * */
  override protected def beforeAll(): Unit = {
    super.beforeAll()
    assume(sourceServerIsAvailable)
    sourceOnFhirClient.batch()
      .entry(_.update(testObservationResource))
      .returnMinimal().asInstanceOf[FhirBatchTransactionRequestBuilder].execute() map { res =>
      res.httpStatus shouldBe StatusCodes.OK
    }
  }

  /**
   * After the tests complete, delete the "example-observation" resource on the source onFHIR server.
   * */
  override protected def afterAll(): Unit = {
    super.afterAll()
    assume(sourceServerIsAvailable)
    sourceOnFhirClient.batch()
      .entry(_.delete("Observation", "example-observation"))
      .returnMinimal().asInstanceOf[FhirBatchTransactionRequestBuilder].execute() map { res =>
      res.httpStatus shouldBe StatusCodes.OK
    }
  }

  /**
   * Tests the mapping job which reads data from the source onFHIR and executes the mapping on this data.
   * It should produce two observation resources.
   * */
  "Observation mapping" should "should read data from Fhir Server source and map it" in {
    assume(sourceServerIsAvailable)
    fhirMappingJobManager.executeMappingTaskAndReturn(mappingJobExecution = FhirMappingJobExecution(mappingTasks = Seq(observationMappingTask), job = fhirMappingJob), sourceSettings = fhirServerSourceSettings) map { mappingResults =>
      val results = mappingResults.map(r => {
        r.mappedResource should not be None
        val resource = r.mappedResource.get.parseJson
        resource shouldBe a[Resource]
        resource
      })
      results.size shouldBe 2
      val observation1 = results.head
      FHIRUtil.extractResourceType(observation1) shouldBe "Observation"
      FHIRUtil.extractValue[String](observation1, "status") shouldBe "final"
      ((observation1 \ "code" \ "coding").extract[JArray].arr.head \ "code").extract[String] shouldBe "8310-5"
      (observation1 \ "valueQuantity" \ "value").extract[Double] shouldBe 37.2
      val observation2 = results.last
      FHIRUtil.extractResourceType(observation2) shouldBe "Observation"
      FHIRUtil.extractValue[String](observation2, "status") shouldBe "final"
      ((observation2 \ "code" \ "coding").extract[JArray].arr.head \ "code").extract[String] shouldBe "9279-1"
      (observation2 \ "valueQuantity" \ "value").extract[Double] shouldBe 18
    }
  }

  /**
   * Executes the mapping job which reads data from the source onFHIR and writes the generated resources to target onFHIR.
   * There should be two Observation resources on the target onFHIR after the mapping job is completed.
   * */
  it should "map test data and write it to FHIR repo successfully" in {
    assume(sourceServerIsAvailable)
    assume(targetServerIsAvailable)
    fhirMappingJobManager
      .executeMappingJob(mappingJobExecution = FhirMappingJobExecution(mappingTasks = Seq(observationMappingTask), job = fhirMappingJob), sourceSettings = fhirServerSourceSettings, sinkSettings = fhirSinkSettings)
      .flatMap(_ => {
        targetOnFhirClient.search("Observation").where("subject", "Patient/example-patient")
          .executeAndReturnBundle() flatMap { obsBundle =>
          obsBundle.searchResults.size shouldBe 2
          var batchRequest: FhirBatchTransactionRequestBuilder = targetOnFhirClient.batch()
          obsBundle.searchResults.foreach(obs =>
            batchRequest = batchRequest.entry(_.delete("Observation", (obs \ "id").extract[String]))
          )
          batchRequest.returnMinimal().asInstanceOf[FhirBatchTransactionRequestBuilder].execute() map { res =>
            res.httpStatus shouldBe StatusCodes.OK
          }
        }
      })
  }
}

