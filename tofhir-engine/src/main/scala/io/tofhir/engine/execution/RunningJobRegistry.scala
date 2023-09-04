package io.tofhir.engine.execution

import org.apache.spark.sql.streaming.StreamingQuery

import java.util.concurrent.{CompletableFuture, ExecutorService, Executors}
import scala.collection.concurrent
import scala.concurrent.Future
import scala.jdk.CollectionConverters.ConcurrentMapHasAsScala

/**
 * Execution manager that keeps track of running mapping tasks in-memory
 */
object RunningJobRegistry {
  // Keeps active executions in the form of: jobId -> (mappingUrl -> streaming query)
  // Using a concurrent map as multiple threads may update same resources in the map.
  private val streams: concurrent.Map[String, concurrent.Map[String, StreamingQuery]] = new java.util.concurrent.ConcurrentHashMap[String, concurrent.Map[String, StreamingQuery]]().asScala

  // Unbounded thread pool that is appropriate for many short-lived asynchronous tasks
  val executor: ExecutorService = Executors.newCachedThreadPool

  /**
   * Caches a [[StreamingQuery]] for an individual mapping task
   *
   * @param jobId                Identifier of the job containing the mapping
   * @param mappingUrl           Url of the mapping
   * @param streamingQueryFuture Future for the streaming query. It resolves into a [[StreamingQuery]] of Spark
   * @return Java Future as a handler for the submitted Runnable task
   */
  def listenStreamingQueryInitialization(jobId: String, mappingUrl: String, streamingQueryFuture: Future[StreamingQuery], blocking: Boolean = false): CompletableFuture[_] = {
    val listenerTask: StreamingQueryListener = new StreamingQueryListener(streamingQueryFuture,
      // Callback method for the future. It gets the initialized StreamingQuery and puts it in the cache
      (streamingQuery: StreamingQuery) => {
        val jobsQueryMap = streams.getOrElseUpdate(jobId, new java.util.concurrent.ConcurrentHashMap[String, StreamingQuery]().asScala)
        jobsQueryMap.put(mappingUrl, streamingQuery)
      },
      blocking
    )
    // Utilize Java's CompletableFuture API to have an actionable handler for the result of the listener task
    CompletableFuture.runAsync(listenerTask, executor)
  }

  /**
   * Stops all [[StreamingQuery]]s associated with a job
   *
   * @param jobId Identifier of the job to be stopped
   */
  def stopJobExecution(jobId: String): Unit = {
    streams.get(jobId) match {
      case None =>
      case Some(map) =>
        map.foreach(sq => {
          sq._2.stop()
        })
        streams.remove(jobId)
    }
  }

  /**
   * Stops the [[StreamingQuery]] associated with a mapping task
   *
   * @param jobId      Identifier of the job containing the mapping
   * @param mappingUrl Url of the mapping
   */
  def stopMappingExecution(jobId: String, mappingUrl: String): Unit = {
    streams.get(jobId) match {
      case None =>
      case Some(map) =>
        map.get(mappingUrl) match {
          case None =>
          case Some(streamingQuery) =>
            streamingQuery.stop()
            map.remove(mappingUrl)
        }
    }
  }

  /**
   * Checks existence of execution for a job or a mapping task
   *
   * @param jobId I
   * @param mappingUrl
   * @return
   */
  def executionExists(jobId: String, mappingUrl: Option[String]): Boolean = {
    mappingUrl match {
      case None => streams.contains(jobId) // Check existence of an execution for a job
      case Some(url) => streams.contains(jobId) && streams(jobId).contains(url) // Check existence of an execution for a mapping
    }
  }

  /**
   * Returns a map of (job -> sequence of urls of running mappings inside the job)
   *
   * @return
   */
  def getRunningExecutions(): Map[String, Seq[String]] = {
    streams.map(entry => entry._1 -> entry._2.keySet.toSeq).toMap
  }
}
