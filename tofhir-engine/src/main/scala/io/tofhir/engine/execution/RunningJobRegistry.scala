package io.tofhir.engine.execution

import it.sauronsoftware.cron4j.{Scheduler, SchedulerListener, TaskExecutor}
import com.typesafe.scalalogging.Logger
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.model.FhirMappingJobExecution
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery

import java.util.UUID
import java.util.concurrent.Executors
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/**
 * Execution manager that keeps track of running and scheduled mapping tasks in-memory.
 * This registry is designed to maintain the execution status of both Streaming and Batch mapping jobs.
 *
 * For Streaming Jobs:
 * - Each task's execution status is tracked individually.
 *
 * For Batch Jobs:
 * - The registry maintains the overall execution status of the Batch job.
 * - Individual statuses of tasks within a batch job are not maintained in the registry due to the nature of batch processing.
 */
class RunningJobRegistry(spark: SparkSession) {
  // Keeps active executions in the form of: jobId -> (executionId -> execution)
  private val runningTasks: collection.mutable.Map[String, collection.mutable.Map[String, FhirMappingJobExecution]] =
    collection.mutable.Map[String, collection.mutable.Map[String, FhirMappingJobExecution]]()

  // Keeps the scheduled jobs in the form of: jobId -> (executionId -> Scheduler)
  private val scheduledTasks: collection.mutable.Map[String, collection.mutable.Map[String, Scheduler]] =
    collection.mutable.Map[String, collection.mutable.Map[String, Scheduler]]()

  // Dedicated execution context for blocking streaming jobs
  private val streamingTaskExecutionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newCachedThreadPool)

  private val logger: Logger = Logger(this.getClass)

  /**
   * Caches a [[FhirMappingJobExecution]] for an individual mapping task
   *
   * @param execution            Execution containing the mapping tasks
   * @param mappingUrl           Specific url which the [[StreamingQuery]] is associated to
   * @param streamingQueryFuture Future for the [[StreamingQuery]]
   * @param blocking             Whether the call will wait or not for the StreamingQuery, passed inside the execution
   * @return
   */
  def registerStreamingQuery(execution: FhirMappingJobExecution, mappingUrl: String, streamingQueryFuture: Future[StreamingQuery], blocking: Boolean = false): Future[Unit] = {
    Future {
      // If there is an error in the streaming query execution, call 'stopMappingExecution' function,
      // which is responsible for removing it from the registry. Without that, the registry might contain incorrect
      // information such as indicating that the job is still running when it has encountered an error.
      streamingQueryFuture.recover(_ => stopMappingExecution(execution.job.id, execution.id, mappingUrl))
      // Wait for the initial Future to be resolved
      val streamingQuery: StreamingQuery = Await.result(streamingQueryFuture, Duration.Inf)
      val jobId: String = execution.job.id
      val executionId: String = execution.id

      // Multiple threads can update the global task map. So, updates are synchronized.
      val updatedExecution = runningTasks.synchronized {
        // Update the execution map
        val executionMap: collection.mutable.Map[String, FhirMappingJobExecution] = runningTasks.getOrElseUpdate(jobId, collection.mutable.Map[String, FhirMappingJobExecution]())
        val updatedExecution: FhirMappingJobExecution = executionMap.get(executionId) match {
          case None => execution.copy(jobGroupIdOrStreamingQuery = Some(Right(collection.mutable.Map(mappingUrl -> streamingQuery))))
          case Some(execution) => execution.copy(jobGroupIdOrStreamingQuery = Some(Right(execution.getStreamingQueryMap() + (mappingUrl -> streamingQuery))))
        }
        executionMap.put(executionId, updatedExecution)
        updatedExecution
      }
      logger.debug(s"Streaming query for execution: $executionId, mappingUrl: $mappingUrl has been registered")

      // If blocking was set true, we are going to wait for StreamingQuery to terminate
      if (blocking) {
        updatedExecution.getStreamingQuery(mappingUrl).awaitTermination()
        // Remove the mapping execution from the running tasks after the query is terminated
        removeMappingExecutionFromRunningTasks(jobId, executionId, mappingUrl)
        () // Return unit
      }

      // Use the dedicated ExecutionContext for streaming jobs
    }(if (blocking) streamingTaskExecutionContext else dispatcher)
  }

  // TODO: Improve cancellation of running batch mapping jobs.
  //  Currently, we rely on setJobGroup and cancelJobGroup functions in Spark to cancel tasks associated with a mapping job.
  //  However, due to Spark's task distribution across multiple threads, using cancelJobGroup may not cancel all tasks,
  //  especially in the case of scheduled mapping jobs because setJobGroup only works for the Spark tasks started in the same thread.
  //  To resolve this issue, we need to assign a unique Spark job group id at the start of mapping job execution, before registering it with the RunningJobRegistry.
  //  We should call setJobGroup function before Spark tasks begin execution. The ideal location for this call seems to be the readSourceExecuteAndWriteInBatches function,
  //  although thorough testing is required to ensure its effectiveness.
  //  UPDATE: Although I set the job group id of each thread to the same value, cancelJobGroup does not work as expected
  //  because it only cancels the active jobs in Spark 3.5 not the future submitted jobs.
  //  In the main branch of Spark, this method can take a parameter named cancelFutureJobs that can resolve our problem.
  //  Please see https://github.com/apache/spark/blob/33a153a6bbcba0d9b2ab20404c7d3b6db86d7b4a/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L1108
  /**
   * Caches a batch job. This method sets the Spark job group id for further referencing (e.g. cancelling the Spark jobs via the job group).
   * Spark job group manages job groups per different threads. This practically means that for each mapping execution request initiated by a REST call would have a different job group.
   * We utilize jobFuture to handle the completion of the job, however, for scheduled mapping jobs, we do not have such a future. Its completion will be handled by scheduler listeners in
   * registerSchedulingJob function.
   *
   * @param execution      Execution representing the batch job.
   * @param jobFuture      Unified Future to yield the completion of the mapping tasks (Optional since scheduling jobs do not have a future).
   * @param jobDescription Job description to be used by Spark. Spark uses it for reporting purposes.
   */
  def registerBatchJob(execution: FhirMappingJobExecution, jobFuture: Option[Future[Unit]], jobDescription: String = ""): Unit = {
    val jobGroup: String = setSparkJobGroup(jobDescription)
    val executionWithJobGroupId = execution.copy(jobGroupIdOrStreamingQuery = Some(Left(jobGroup)))
    val jobId: String = executionWithJobGroupId.job.id
    val executionId: String = executionWithJobGroupId.id

    runningTasks.synchronized {
      runningTasks
        .getOrElseUpdate(jobId, collection.mutable.Map[String, FhirMappingJobExecution]())
        .put(executionId, executionWithJobGroupId)

      logger.debug(s"Batch job for execution: $executionId has been registered with spark job group id: $jobGroup")
    }
    // Remove the execution entry when the future is completed
    if(jobFuture.nonEmpty)
      jobFuture.get.onComplete(_ => {
        handleCompletedBatchJob(execution)
      })
  }

  /**
   * Registers a scheduling job with the specified mapping job execution and scheduler.
   *
   * @param mappingJobExecution The mapping job execution.
   * @param scheduler           The scheduler associated with the job execution.
   */
  def registerSchedulingJob(mappingJobExecution: FhirMappingJobExecution, scheduler: Scheduler): Unit = {
    // add it to the scheduledTasks map
    scheduledTasks
      .getOrElseUpdate(mappingJobExecution.job.id, collection.mutable.Map[String, Scheduler]())
      .put(mappingJobExecution.id, scheduler)
    logger.debug(s"Scheduling job ${mappingJobExecution.job.id} has been registered")
    // add a scheduler listener to monitor task events
    scheduler.addSchedulerListener(new SchedulerListener {
      override def taskLaunching(executor: TaskExecutor): Unit = {
        registerBatchJob(mappingJobExecution,None,s"Spark job for job: ${mappingJobExecution.job.id} mappings: ${mappingJobExecution.mappingTasks.map(_.mappingRef).mkString(" ")}")
      }

      override def taskSucceeded(executor: TaskExecutor): Unit = {
        handleCompletedBatchJob(mappingJobExecution)
      }

      override def taskFailed(executor: TaskExecutor, exception: Throwable): Unit = {
        handleCompletedBatchJob(mappingJobExecution)
      }
    })
  }

  /**
   * Deschedules a job execution.
   *
   * @param jobId       The ID of the job.
   * @param executionId The ID of the execution.
   */
  def descheduleJobExecution(jobId: String, executionId: String): Unit = {
    // TODO: We call this function but it does not actually stop the execution of a scheduled mappings jobs
    //  due to the fact that Spark can distribute the tasks into several threads and our setJobGroup/cancelJobGroup
    //  logic cannot work properly in this case
    // stop the job execution
    stopJobExecution(jobId, executionId)
    // stop the scheduler for the specified job execution
    scheduledTasks(jobId)(executionId).stop()
    logger.debug(s"Descheduled the mapping job with id: $jobId and execution: $executionId")
    // remove the execution from the scheduledTask Map
    scheduledTasks(jobId).remove(executionId)
    // if there are no executions left for the job, remove the job from the map
    if(!scheduledTasks.contains(jobId)){
      scheduledTasks.remove(jobId)
    }
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
      case Some(execution) =>
        execution.jobGroupIdOrStreamingQuery.get match {
          // For batch jobs, we cancel the job group.
          case Left(sparkJobGroup) =>
            spark.sparkContext.cancelJobGroup(sparkJobGroup)
            logger.debug(s"Canceled Spark job group with id: $sparkJobGroup")

          // For streaming jobs, we terminate the streaming queries one by one
          case Right(queryMap) =>
            queryMap.foreach(queryEntry => {
              queryEntry._2.stop()
              logger.debug(s"Stopped streaming query for mapping: ${queryEntry._1}")
            })
        }
    }
  }

  /**
   * Stops all the executions of a mapping job with the specified jobId.
   *
   * @param jobId The identifier of the mapping job for which executions should be stopped.
   */
  def stopJobExecutions(jobId: String): Unit = {
    runningTasks.get(jobId) match {
      case None => // No running tasks for the specified jobId, nothing to do
      case Some(executionsMap) =>
        // Stop each individual execution associated with the jobId
        executionsMap.values.toSeq.foreach(execution => removeExecutionFromRunningTasks(jobId,execution.id))
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
        case Right(streamingQuery) => streamingQuery.stop()
      }
    }
  }

  /**
   * Removes the entry from the running tasks map for the given job and execution. If the removed entry is the last one for the given job,
   * the complete job entry is also removed.
   *
   * @param jobId       Identifier of the job
   * @param executionId Identifier of the execution
   * @return Returns the removed mapping entry if at all (executionId -> execution), or None
   */
  private def removeExecutionFromRunningTasks(jobId: String, executionId: String): Option[FhirMappingJobExecution] = {
    runningTasks.synchronized {
      runningTasks.get(jobId) match {
        case Some(jobMapping) if jobMapping.contains(executionId) =>
          val removedExecutionEntry = jobMapping.remove(executionId)
          // Remove the job mappings completely if it is empty
          if (runningTasks(jobId).isEmpty) {
            runningTasks.remove(jobId)
          }
          removedExecutionEntry
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
        case Some(jobMapping) if jobMapping.contains(executionId) =>
          val execution: FhirMappingJobExecution = jobMapping(executionId)
          var removedMappingEntry: Option[Either[String, StreamingQuery]] = None
          // If it is a batch job do nothing but warn user about the situation
          if (!execution.isStreaming()) {
            logger.warn(s"Execution with $jobId: $jobId, executionId: $executionId, mappingUrl: $mappingUrl won't be stopped with a specific mapping as this is a batch job." +
              s"Stop execution by providing only the jobId and executionId")

            // Streaming query
          } else {
            if (execution.getStreamingQueryMap().contains(mappingUrl)) {
              removedMappingEntry = Some(Right(execution.getStreamingQueryMap().remove(mappingUrl).get))
              // Remove the execution mappings completely if it is empty
              if(execution.getStreamingQueryMap().isEmpty) {
                jobMapping.remove(executionId)
                // Remove the job mappings completely if it is empty
                if (runningTasks(jobId).isEmpty) {
                  runningTasks.remove(jobId)
                }
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
   * @param jobId       Identifier of the job associated with the execution
   * @param executionId Identifier of the execution to be stopped
   * @param mappingUrl  Url of the mapping representing the mapping task being executed
   * @return
   */
  def executionExists(jobId: String, executionId: String, mappingUrl: Option[String]): Boolean = {
    if (runningTasks.contains(jobId) && runningTasks(jobId).contains(executionId)) {
      mappingUrl match {
        case None => true // We know we have an execution at this point
        case Some(url) =>
          // For streaming jobs, we check whether there is a streaming query for the given mapping
          if (runningTasks(jobId)(executionId).isStreaming()) {
            runningTasks(jobId)(executionId).getStreamingQueryMap().contains(url)

            // For batch jobs, we don't differentiate mapping tasks. So, returning true directly (which indicates that the job execution is in progress)
          } else {
            true
          }
      }
    } else {
      false
    }
  }

  /**
   * Checks whether a mapping job with the specified jobId is currently running.
   *
   * This method determines if a mapping job is running by checking if there are
   * any active executions associated with the specified jobId.
   *
   * @param jobId The identifier of the mapping job to be checked for running status.
   * @return `true` if the specified mapping job is running, otherwise `false`.
   */
  def isJobRunning(jobId: String): Boolean = {
    runningTasks.contains(jobId)
  }

  /**
   * Gets running executions for the given job
   *
   * @param jobId Identifier of the job
   * @return A set of execution ids
   */
  def getRunningExecutions(jobId: String): Set[String] = {
    runningTasks.get(jobId).map(_.keySet).getOrElse(Set.empty).toSet
  }

  /**
   * Checks if a job with the given execution ID is scheduled.
   *
   * @param jobId       The ID of the job
   * @param executionId The ID of the execution
   * @return True if the given job execution is scheduled, otherwise false.
   */
  def isScheduled(jobId: String, executionId: String): Boolean = {
    scheduledTasks.contains(jobId) && scheduledTasks(jobId).contains(executionId)
  }

  /**
   * Converts the running task map into a structure as follows: (jobId -> sequence of (executionId -> sequence of mapping urls))
   *
   * @return
   */
  def getRunningExecutions(): Map[String, Seq[(String, Seq[String])]] = {
      runningTasks.map(entry =>
        entry._1 -> // jobId
          entry._2 // a map in the form of (executionId -> FhirMappingJobExecution)
            .map(executionMappings => (executionMappings._1, executionMappings._2.mappingTasks.map(_.mappingRef))) // (executionId -> sequence of mapping urls)
            .toSeq
      ).toMap
  }

  /**
   * Returns [[FhirMappingJobExecution]]s for all the running executions
   *
   * @return
   */
  def getRunningExecutionsWithCompleteMetadata(): Seq[FhirMappingJobExecution] = {
    runningTasks
      .flatMap(_._2.values) // concatenate executions of all jobs
      .toSeq
  }

  /**
   * Sets the Spark job group for the active thread, which is used to uniquely identify and manage
   * batch jobs. This is particularly useful for closing mapping tasks associated with a specific job.
   *
   * @param description Description for the job group
   * @return The generated job group id
   */
  private def setSparkJobGroup(description: String = ""): String = {
    val newJobGroup: String = UUID.randomUUID().toString
    spark.sparkContext.setJobGroup(newJobGroup, description, true)
    newJobGroup
  }

  /**
   * Handles the completion of a batch job execution.
   * This method runs archiving manually for the batch job and removes the execution from the list of running tasks.
   *
   * @param execution The execution of the batch job to handle.
   */
  private def handleCompletedBatchJob(execution: FhirMappingJobExecution): Unit = {
    FileStreamInputArchiver.applyArchivingOnBatchJob(execution)
    removeExecutionFromRunningTasks(execution.job.id, execution.id)
  }
}
