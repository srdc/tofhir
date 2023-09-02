package io.tofhir.engine.execution

import org.apache.spark.sql.streaming.StreamingQuery

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
 * A runnable to listen indefinitely for the given StreamingQuery Future to be resolved.
 * Moreover, if the blocking is set true, the resolved streaming query will be waited indefinitely to complete.
 *
 * @param streamingQueryFuture StreamingQuery future
 * @param callback             Method to be called once the future is complete
 * @param blocking             Whether the streaming query should be waited indefinitely to complete
 */
class StreamingQueryListener(streamingQueryFuture: Future[StreamingQuery], callback: StreamingQuery => Unit, blocking: Boolean) extends Runnable {
  override def run(): Unit = {
    val streamingQuery: StreamingQuery = Await.result(streamingQueryFuture, Duration.Inf)
    callback(streamingQuery)
    if (blocking) {
      streamingQuery.awaitTermination()
    }
  }
}
