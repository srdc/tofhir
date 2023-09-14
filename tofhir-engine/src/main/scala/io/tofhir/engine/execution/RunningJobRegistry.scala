package io.tofhir.engine.execution

import akka.parboiled2.RuleTrace.Fail
import com.typesafe.scalalogging.Logger
import io.tofhir.engine.Execution.actorSystem.dispatcher
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery

import java.util.UUID
import java.util.concurrent.Executors
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.runtime.Nothing$
import scala.util.{Failure, Success}

/**
 * Execution manager that keeps track of running mapping tasks in-memory
 */
class RunningJobRegistry(spark: SparkSession) {
  // Keeps active executions in the form of: jobId -> (executionId -> (mappingUrl -> (spark job group or streaming query)))
  private val runningTasks: collection.mutable.Map[String, collection.mutable.Map[String, collection.mutable.Map[String, Either[String, StreamingQuery]]]] =
    collection.mutable.Map[String, collection.mutable.Map[String, collection.mutable.Map[String, Either[String, StreamingQuery]]]]()

  // Dedicated execution context for blocking streaming jobs
  private val streamingTaskExecutionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newCachedThreadPool)

  private val logger: Logger = Logger(this.getClass)

  /**
   * Caches a [[StreamingQuery]] for an individual mapping task
   *
   * @param jobId                Identifier of the job containing the mapping task
   * @param executionId          Identifier of the execution associated with the mapping task
   * @param mappingUrl           Url of the mapping
   * @param streamingQueryFuture Future for the streaming query. It resolves into a [[StreamingQuery]] of Spark
   * @return Java Future as a handler for the submitted Runnable task
   */
  def registerStreamingQuery(jobId: String, executionId: String, mappingUrl: String, streamingQueryFuture: Future[StreamingQuery], blocking: Boolean = false): Future[Unit] = {
    Future {
      // Wait for the initial Future to be resolved
      val streamingQuery: StreamingQuery = Await.result(streamingQueryFuture, Duration.Inf)

      // Multiple threads can update the global task map. So, updates are synchronized.
      runningTasks.synchronized {
        // Update the StreamingQuery map
        getOrinitializeExecutionMappings(jobId, executionId).put(mappingUrl, Right(streamingQuery))
      }

      // If blocking was set true, we are going to wait for StreamingQuery to terminate
      if (blocking) {
        streamingQuery.awaitTermination()
        // Remove the mapping execution from the running tasks after the query is terminated
        removeMappingExecutionFromRunningTasks(jobId, executionId, mappingUrl)
        () // Return unit
      }

      // Use the dedicated ExecutionContext for streaming jobs
    }(if (blocking) streamingTaskExecutionContext else dispatcher)
  }

  /**
   * Caches a batch job. This method sets the Spark job group id for further referencing (e.g. cancelling the Spark jobs via the job group).
   * Spark job group manages job groups per different threads. This practically means that for each mapping execution request initiated by a REST call would have a different job group.
   *
   * @param jobId          Identifier of the job containing the mapping tasks
   * @param executionId    Identifier of the execution associated with the mapping tasks
   * @param mappingUrls    Urls of the mappings running in the given job
   * @param jobFuture      Unified Future to yield the completion of the mapping tasks
   * @param jobDescription Job description to be used by Spark. Spark uses it for reporting purposes
   */
  def registerBatchJob(jobId: String, executionId: String, mappingUrls: Seq[String], jobFuture: Future[Unit], jobDescription: String = ""): Unit = {
    val jobGroup: String = setSparkJobGroup(jobDescription)
    runningTasks.synchronized {
      // Each mapping task is added to the running task map. This allows us to query running status of individual mappings
      mappingUrls.foreach(url => {
        getOrinitializeExecutionMappings(jobId, executionId).put(url, Left(jobGroup))
      })
    }
    // Remove the execution entry when the future is completed
    jobFuture.onComplete(_ => {
      removeExecutionFromRunningTasks(jobId, executionId)
    })
  }

  /**
   * Utility method to initialize the nested execution map for the given job and execution.
   *
   * @param jobId
   * @param executionId
   * @return The execution mappings i.e. (mapping url -> (spark job group id | streaming query))
   */
  private def getOrinitializeExecutionMappings(jobId: String, executionId: String): collection.mutable.Map[String, Either[String, StreamingQuery]] = {
    runningTasks
      .getOrElseUpdate(jobId, collection.mutable.Map[String, collection.mutable.Map[String, Either[String, StreamingQuery]]]())
      .getOrElseUpdate(executionId, collection.mutable.Map[String, Either[String, StreamingQuery]]())
  }

  /**
   * Stops all [[StreamingQuery]]s associated with the specified execution.
   *
   * @param jobId       Identifier of the job associated with the execution
   * @param executionId Identifier of the execution to be stopped
   */
  def stopJobExecution(jobId: String, executionId: String): Unit = {
    removeExecutionFromRunningTasks(jobId, executionId) match {
      case None => // Nothing to do
      case Some(executionMappings) =>
        executionMappings.head._2 match {
          // For batch jobs, we cancel the job group.
          case Left(sparkJobGroup) =>
            spark.sparkContext.cancelJobGroup(sparkJobGroup)
            logger.debug(s"Canceled Spark job group with id: $sparkJobGroup")

          // For streaming jobs, we terminate the streaming queries one by one
          case Right(_) =>
            executionMappings.foreach(value => {
              value._2.toOption.get.stop()
              logger.debug(s"Stopped streaming query for mapping: ${value._1}")
            })
        }
    }
  }

  /**
   * Stops the [[StreamingQuery]] associated with an individual mapping task
   *
   * @param jobId       Identified of the associated with the execution
   * @param executionId Identifier of the job containing the mapping
   * @param mappingUrl  Url of the mapping
   */
  def stopMappingExecution(jobId: String, executionId: String, mappingUrl: String): Unit = {
    removeMappingExecutionFromRunningTasks(jobId, executionId, mappingUrl) match {
      case None => // Nothing to do
      case Some(result) => result match {
        case Left(sparkJobGroup) => spark.sparkContext.cancelJobGroup(sparkJobGroup)
        case Right(streamingQuery) => streamingQuery.stop
      }
    }
  }

  /**
   * Removes the entry from the running tasks map for the given job and execution. If the removed entry is the last one for the given job,
   * the complete job entry is also removed.
   *
   * @param jobId       Identifier of the job
   * @param executionId Identifier of the execution
   * @return Returns the removed mapping entry if at all (executionId -> (mapping url -> (spark job group id | streaming query))), or None
   */
  private def removeExecutionFromRunningTasks(jobId: String, executionId: String): Option[mutable.Map[String, Either[String, StreamingQuery]]] = {
    runningTasks.synchronized {
      runningTasks.get(jobId) match {
        case Some(jobMapping) if jobMapping.contains(executionId) =>
          val removedExecutionMappingsEntry = jobMapping.remove(executionId)
          // Remove the job mappings completely if it is empty
          if (runningTasks(jobId).isEmpty) {
            runningTasks.remove(jobId)
          }
          removedExecutionMappingsEntry
        case _ => None
      }
    }
  }

  /**
   * Removes the entry from the running tasks map for the given job, execution and mapping. If the removed entry is the last one for the given execution, the execution itself is also removed.
   * Furthermore, if the execution is the last one for the given job, the job entry is also removed.
   *
   * @param jobId       Identifier of the job
   * @param executionId Identifier of the execution
   * @param mappingUrl  Url of the mapping
   * @return Returns the removed mapping entry if at all or None
   */
  private def removeMappingExecutionFromRunningTasks(jobId: String, executionId: String, mappingUrl: String): Option[Either[String, StreamingQuery]] = {
    runningTasks.synchronized {
      runningTasks.get(jobId) match {
        case Some(jobMapping) if jobMapping.contains(executionId) && jobMapping(executionId).contains(mappingUrl) =>
          var removedMappingEntry: Option[Either[String, StreamingQuery]] = None
          // If it is a batch job do nothing but warn user about the situation
          if (jobMapping(executionId)(mappingUrl).isLeft) {
            logger.warn(s"Execution with $jobId: $jobId, executionId: $executionId, mappingUrl: $mappingUrl won't be stopped with a specific mapping as this is a batch job." +
              s"Stop execution by providing only the jobId and executionId")

          } else {
            removedMappingEntry = jobMapping(executionId).remove(mappingUrl)
            // Remove the execution mappings completely if it is empty
            if (jobMapping(executionId).isEmpty) {
              jobMapping.remove(executionId)
              // Remove the job mappings completely if it is empty
              if (runningTasks(jobId).isEmpty) {
                runningTasks.remove(jobId)
              }
            }
          }
          removedMappingEntry
        case _ => None
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
  def executionExists(jobId: String, executionId: String, mappingUrl: Option[String]): Boolean = {
    if (runningTasks.contains(jobId) && runningTasks(jobId).contains(executionId)) {
      mappingUrl match {
        case None => true // We know we have an execution at this point
        case Some(url) => runningTasks(jobId)(executionId).contains(url)
      }
    } else {
      false
    }
  }

  /**
   * Converts the running task map into a structure as follows: (jobId -> sequence of (executionId -> sequence of mapping urls))
   *
   * @return
   */
  def getRunningExecutions(): Map[String, Seq[(String, Seq[String])]] = {
      runningTasks.map(entry =>
        entry._1 -> // jobId
          entry._2 // a map in the form of (executionId -> (mapping url -> (streaming query | spark job group id )))
            .map(executionMappings => (executionMappings._1, executionMappings._2.keys.toSeq)) // (executionId -> sequence of mapping urls)
            .toSeq
      ).toMap
  }

  /**
   * Sets the Spark's job group for the active thread.
   *
   * @param description Description for the job group
   * @return The generated job group id
   */
  def setSparkJobGroup(description: String = ""): String = {
    val newJobGroup: String = UUID.randomUUID().toString
    spark.sparkContext.setJobGroup(newJobGroup, description, true)
    newJobGroup
  }
}
