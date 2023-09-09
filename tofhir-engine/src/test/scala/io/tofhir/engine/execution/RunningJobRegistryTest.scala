package io.tofhir.engine.execution

import akka.actor.ActorSystem
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery
import org.mockito.MockitoSugar._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.postfixOps

class RunningJobRegistryTest extends AnyFlatSpec with Matchers {

  implicit val actorSystem: ActorSystem = ActorSystem("toFhirEngineTest")
  implicit val executionContext: ExecutionContext = actorSystem.getDispatcher
  val runningTaskRegistry: RunningJobRegistry = new RunningJobRegistry(mock[SparkSession])

  "InMemoryExecutionManager" should "cache a StreamingQuery" in {
    val streamingQueryFuture = getMockStreamingQuery()
    runningTaskRegistry.registerStreamingQuery("j", "e", "m", streamingQueryFuture)

    // The streaming future is resolved inside the listener. 2 seconds waiting time seems safe for runnable to be submitted and
    // future to be resolved. It can be adjusted though.
    Await.result(streamingQueryFuture, 2 seconds)
    // The listener thread initiated by the execution manager calls an asynchronous callback. Below, we wait it to run.
    Thread.sleep(100)
    runningTaskRegistry.getRunningExecutions().size shouldBe 1
  }

  "it" should "stop all StreamingQueries associated with a job" in {
    val streamingQueryFuture = getMockStreamingQuery()
    val streamingQueryFuture2 = getMockStreamingQuery()
    runningTaskRegistry.registerStreamingQuery("j", "e", "m2", streamingQueryFuture)
    runningTaskRegistry.registerStreamingQuery("j", "e", "m3", streamingQueryFuture2)

    // The streaming future is resolved inside the listener. 2 seconds waiting time seems safe for runnable to be submitted and
    // future to be resolved. It can be adjusted though.
    val streamingQuery = Await.result(streamingQueryFuture, 2 seconds)
    val streamingQuery2 = Await.result(streamingQueryFuture2, 2 seconds)

    // The listener thread initiated by the execution manager calls an asynchronous callback. Below, we wait it to run.
    Thread.sleep(100)

    runningTaskRegistry.stopJobExecution("j", "e")
    verify(streamingQuery).stop()
    verify(streamingQuery2).stop()
    runningTaskRegistry.getRunningExecutions().size shouldBe 0
  }

  "InMemoryExecutionManager" should "stop StreamingQuery associated with a mapping" in {
    val streamingQueryFuture = getMockStreamingQuery()
    runningTaskRegistry.registerStreamingQuery("j", "e", "m", streamingQueryFuture)

    // The streaming future is resolved inside the listener. 2 seconds waiting time seems safe for runnable to be submitted and
    // future to be resolved. It can be adjusted though.
    val streamingQuery = Await.result(streamingQueryFuture, 2 seconds)
    // The listener thread initiated by the execution manager calls an asynchronous callback. Below, we wait it to run.
    Thread.sleep(100)

    runningTaskRegistry.stopMappingExecution("j", "e", "m")
    verify(streamingQuery).stop()
    runningTaskRegistry.getRunningExecutions().size shouldBe 1
  }

  private def getMockStreamingQuery(): Future[StreamingQuery] = {
    Future.apply(mock[StreamingQuery])
  }
}
