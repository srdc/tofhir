package io.tofhir.test.engine.execution

import akka.actor.ActorSystem
import io.tofhir.engine.execution.RunningJobRegistry
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.MockitoSugar._
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
    val jobSubmissionFuture = runningTaskRegistry.registerStreamingQuery("j", "e", "m", getMockStreamingQuery())

    Await.result(jobSubmissionFuture, 10 seconds)
    runningTaskRegistry.getRunningExecutions().size shouldBe 1
  }

  "it" should "cache a StreamingQuery in blocking mode" in {
    val streamingQueryFuture = getMockStreamingQuery()
    val jobSubmissionFuture = runningTaskRegistry.registerStreamingQuery("j", "e", "m2", streamingQueryFuture, true)

    Await.result(jobSubmissionFuture, 2 seconds)
    val streamingQuery = Await.result(streamingQueryFuture, 2 seconds)
    verify(streamingQuery).awaitTermination()

    // Registered task should have been deleted after termination
    runningTaskRegistry.getRunningExecutions().size shouldBe 1
  }

  "it" should "stop all StreamingQueries associated with a job" in {
    val streamingQueryFuture = getMockStreamingQuery()
    val streamingQueryFuture2 = getMockStreamingQuery()
    val taskFuture1 = runningTaskRegistry.registerStreamingQuery("j2", "e", "m1", streamingQueryFuture)
    val taskFuture2 = runningTaskRegistry.registerStreamingQuery("j2", "e", "m2", streamingQueryFuture2)

    Await.result(taskFuture1, 10 seconds)
    Await.result(taskFuture2, 10 seconds)

    runningTaskRegistry.stopJobExecution("j2", "e")
    verify(streamingQueryFuture.value.get.get).stop()
    verify(streamingQueryFuture2.value.get.get).stop()
    runningTaskRegistry.getRunningExecutions().contains("j2") shouldBe false
  }

  "it" should "stop StreamingQuery associated with a mapping" in {
    val streamingQueryFuture = getMockStreamingQuery()
    val taskFuture = runningTaskRegistry.registerStreamingQuery("j3", "e", "m", streamingQueryFuture)

    Await.result(taskFuture, 10 seconds)

    runningTaskRegistry.stopMappingExecution("j3", "e", "m")
    verify(streamingQueryFuture.value.get.get).stop()
    runningTaskRegistry.getRunningExecutions().contains("j3") shouldBe false
  }

  "it" should "register batch jobs" in {
    runningTaskRegistry.registerBatchJob("j4", "e", Seq("m1", "m2"), Future.apply(
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

  private def getMockStreamingQuery(): Future[StreamingQuery] = {
    Future.apply(mock[StreamingQuery])
  }

  private def getMockSparkSession(): SparkSession = {
    val mockSparkSession: SparkSession = mock[SparkSession]
    when(mockSparkSession.sparkContext).thenReturn(mock[SparkContext])
  }
}
