package io.tofhir.engine.execution

import akka.actor.ActorSystem
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

  "InMemoryExecutionManager" should "cache a StreamingQuery" in {
    val streamingQueryFuture = getMockStreamingQuery()
    RunningJobRegistry.listenStreamingQueryInitialization("j", "m", streamingQueryFuture)
    // The streaming future is resolved inside the listener. 2 seconds waiting time seems safe for runnable to be submitted and
    // future to be resolved. It can be adjusted though.
    Await.result(streamingQueryFuture, 2 seconds)
    // The listener thread initiated by the execution manager calls an asynchronous callback. Below, we wait it to run.
    Thread.sleep(100)
    RunningJobRegistry.getRunningExecutions().size shouldBe 1
  }

  "it" should "stop all StreamingQueries associated with a job" in {
    val streamingQueryFuture = getMockStreamingQuery()
    val streamingQueryFuture2 = getMockStreamingQuery()
    RunningJobRegistry.listenStreamingQueryInitialization("j", "m2", streamingQueryFuture)
    RunningJobRegistry.listenStreamingQueryInitialization("j", "m3", streamingQueryFuture2)
    // The streaming future is resolved inside the listener. 2 seconds waiting time seems safe for runnable to be submitted and
    // future to be resolved. It can be adjusted though.
    val streamingQuery = Await.result(streamingQueryFuture, 2 seconds)
    val streamingQuery2 = Await.result(streamingQueryFuture2, 2 seconds)
    // The listener thread initiated by the execution manager calls an asynchronous callback. Below, we wait it to run.
    Thread.sleep(100)
    RunningJobRegistry.stopJobExecution("j")
    verify(streamingQuery).stop()
    verify(streamingQuery2).stop()
    RunningJobRegistry.getRunningExecutions().size shouldBe 0
  }

  "InMemoryExecutionManager" should "stop StreamingQuery associated with a mapping" in {
    val streamingQueryFuture = getMockStreamingQuery()
    RunningJobRegistry.listenStreamingQueryInitialization("j", "m", streamingQueryFuture)
    // The streaming future is resolved inside the listener. 2 seconds waiting time seems safe for runnable to be submitted and
    // future to be resolved. It can be adjusted though.
    val streamingQuery = Await.result(streamingQueryFuture, 2 seconds)
    // The listener thread initiated by the execution manager calls an asynchronous callback. Below, we wait it to run.
    Thread.sleep(100)
    RunningJobRegistry.stopMappingExecution("j", "m")
    verify(streamingQuery).stop()
    RunningJobRegistry.getRunningExecutions().size shouldBe 1
  }

  private def getMockStreamingQuery(): Future[StreamingQuery] = {
    Future.apply(mock[StreamingQuery])
  }
}
