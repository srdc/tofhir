package io.tofhir.test.engine.execution

import akka.actor.ActorSystem
import io.tofhir.engine.execution.RunningJobRegistry
import io.tofhir.engine.model.{FhirMappingJob, FhirMappingJobExecution, FhirMappingTask, FileSystemSourceSettings}
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

  "RunningJobRegistryTest" should "cache a StreamingQuery" in {
    val input = getTestInput("j", "e", Seq("m"))
    val jobSubmissionFuture = runningTaskRegistry.registerStreamingQuery(input._1, input._2.head, input._3)

    Await.result(jobSubmissionFuture, 10 seconds)
    runningTaskRegistry.getRunningExecutions().size shouldBe 0
  }

  "it" should "stop all StreamingQueries associated with a job" in {
    val input1 = getTestInput("j2", "e", Seq("m1"))
    val input2 = getTestInput("j2", "e", Seq("m2"))
    val streamingQueryFuture = input1._3
    val streamingQueryFuture2 = input2._3
    val taskFuture1 = runningTaskRegistry.registerStreamingQuery(input1._1, input1._2.head, streamingQueryFuture)
    val taskFuture2 = runningTaskRegistry.registerStreamingQuery(input2._1, input2._2.head, streamingQueryFuture2)

    Await.result(taskFuture1, 10 seconds)
    Await.result(taskFuture2, 10 seconds)

    runningTaskRegistry.stopJobExecution("j2", "e")
    verify(streamingQueryFuture.value.get.get).stop()
    verify(streamingQueryFuture2.value.get.get).stop()
    runningTaskRegistry.getRunningExecutions().contains("j2") shouldBe false
  }

  "it" should "stop StreamingQuery associated with a mapping" in {
    val input = getTestInput("j3", "e", Seq("m"))
    val taskFuture = runningTaskRegistry.registerStreamingQuery(input._1, input._2.head, input._3)

    Await.result(taskFuture, 10 seconds)

    runningTaskRegistry.stopMappingExecution("j3", "e", "m")
    verify(input._3.value.get.get).stop()
    runningTaskRegistry.getRunningExecutions().contains("j3") shouldBe false
  }

  "it" should "register batch jobs" in {
    val input = getTestInput("j4", "e", Seq("m1", "m2"), false)
    runningTaskRegistry.registerBatchJob(input._1, Some(Future.apply(
      Thread.sleep(1000)
    )), "")
    runningTaskRegistry.getRunningExecutions()("j4").head._2 shouldEqual Seq("m1", "m2")

    val booleanCapturer: ArgumentCaptor[Boolean] = ArgumentCaptor.forClass(classOf[Boolean])
    verify(mockSparkSession.sparkContext).setJobGroup(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), booleanCapturer.capture())
    booleanCapturer.getValue shouldBe true

    // Wait for the Future to complete
    Thread.sleep(1500)

    // Entry for the job and execution should have removed after the future completes
    runningTaskRegistry.getRunningExecutions().contains("j4") shouldBe false
  }

  private def getTestInput(jobId: String, executionId: String, mappingTaskNames: Seq[String], isStream: Boolean = true): (FhirMappingJobExecution, Seq[String], Future[StreamingQuery]) = {
    (
      FhirMappingJobExecution(
        id = executionId,
        job = FhirMappingJob(id = jobId, sourceSettings = Map("s" -> FileSystemSourceSettings("n", "s", "d", isStream)), sinkSettings = null, mappings = Seq.empty),
        mappingTasks = mappingTaskNames.map(name => FhirMappingTask(name, s"http://${name}", Map.empty))
      ),
      mappingTaskNames,
      Future.apply(
        mock[StreamingQuery]
      )
    )
  }

  private def getMockSparkSession(): SparkSession = {
    val mockSparkSession: SparkSession = mock[SparkSession]
    when(mockSparkSession.sparkContext).thenReturn(mock[SparkContext])
  }
}
