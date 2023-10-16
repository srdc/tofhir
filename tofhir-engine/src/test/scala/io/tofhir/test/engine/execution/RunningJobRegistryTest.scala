package io.tofhir.test.engine.execution

import akka.actor.ActorSystem
import io.tofhir.engine.execution.RunningJobRegistry
import io.tofhir.engine.model.{FhirMappingJob, FhirMappingJobExecution, FhirMappingTask}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery
import org.mockito.MockitoSugar._
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.postfixOps

class RunningJobRegistryTest extends AnyFlatSpec with Matchers {

  implicit val actorSystem: ActorSystem = ActorSystem("toFhirEngineTest")
  implicit val executionContext: ExecutionContext = actorSystem.getDispatcher
  val mockSparkSession: SparkSession = getMockSparkSession()
  val runningTaskRegistry: RunningJobRegistry = new RunningJobRegistry(mockSparkSession)

  "InMemoryExecutionManager" should "cache a StreamingQuery" in {
    val jobSubmissionFuture = runningTaskRegistry.registerStreamingQuery(getMockExecution("j", "e", Seq("m")))

    Await.result(jobSubmissionFuture, 10 seconds)
    runningTaskRegistry.getRunningExecutions().size shouldBe 1
  }

  "it" should "cache a StreamingQuery in blocking mode" in {
    val executionFuture = getMockExecution("j", "e", Seq("m2"))
    val jobSubmissionFuture = runningTaskRegistry.registerStreamingQuery(executionFuture, true)

    Await.result(jobSubmissionFuture, 2 seconds)
    val streamingQuery = Await.result(executionFuture, 2 seconds).getStreamingQuery()
    verify(streamingQuery).awaitTermination()

    // Registered task should have been deleted after termination
    runningTaskRegistry.getRunningExecutions().size shouldBe 1
  }

  "it" should "stop all StreamingQueries associated with a job" in {
    val executionFuture = getMockExecution("j2", "e", Seq("m1"))
    val executionFuture2 = getMockExecution("j2", "e", Seq("m2"))
    val taskFuture1 = runningTaskRegistry.registerStreamingQuery(executionFuture)
    val taskFuture2 = runningTaskRegistry.registerStreamingQuery(executionFuture2)

    Await.result(taskFuture1, 10 seconds)
    Await.result(taskFuture2, 10 seconds)

    runningTaskRegistry.stopJobExecution("j2", "e")
    verify(executionFuture.value.get.get).getStreamingQuery().stop()
    verify(executionFuture2.value.get.get).getStreamingQuery().stop()
    runningTaskRegistry.getRunningExecutions().contains("j2") shouldBe false
  }

  "it" should "stop StreamingQuery associated with a mapping" in {
    val executionFuture = getMockExecution("j3", "e", Seq("m"))
    val taskFuture = runningTaskRegistry.registerStreamingQuery(executionFuture)

    Await.result(taskFuture, 10 seconds)

    runningTaskRegistry.stopMappingExecution("j3", "e", "m")
    verify(executionFuture.value.get.get).getStreamingQuery().stop()
    runningTaskRegistry.getRunningExecutions().contains("j3") shouldBe false
  }

  "it" should "register batch jobs" in {
    runningTaskRegistry.registerBatchJob(Await.result(getMockExecution("j4", "e", Seq("m1", "m2")), 1 seconds), Future.apply(
      Thread.sleep(1000)
    ), "")
    runningTaskRegistry.getRunningExecutions()("j4").head._2 shouldEqual Seq("m1", "m2")

    val booleanCapturer: ArgumentCaptor[Boolean] = ArgumentCaptor.forClass(classOf[Boolean])
    verify(mockSparkSession.sparkContext).setJobGroup(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), booleanCapturer.capture())
    booleanCapturer.getValue shouldBe true

    // Wait for the Future to complete
    Thread.sleep(1500)

    // Entry for the job and execution should have removed after the future completes
    runningTaskRegistry.getRunningExecutions().contains("j4") shouldBe false
  }

  private def getMockExecution(jobId: String, executionId: String, mappingUrls: Seq[String]): Future[FhirMappingJobExecution] = {
    Future.apply(FhirMappingJobExecution(
      id = executionId,
      job = FhirMappingJob(id = jobId, sourceSettings = Map.empty, sinkSettings = null, mappings = Seq.empty),
      mappingTasks = mappingUrls.map(url => FhirMappingTask(url, Map.empty)),
      jobGroupIdOrStreamingQuery = Some(Right(mock[StreamingQuery]))
    ))
  }

  private def getMockSparkSession(): SparkSession = {
    val mockSparkSession: SparkSession = mock[SparkSession]
    when(mockSparkSession.sparkContext).thenReturn(mock[SparkContext])
  }
}
