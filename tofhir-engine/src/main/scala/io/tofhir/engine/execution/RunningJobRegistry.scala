package io.tofhir.engine.execution

import it.sauronsoftware.cron4j.{Scheduler, SchedulerListener, TaskExecutor}
import com.typesafe.scalalogging.Logger
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.model.{FhirMappingJobExecution, FhirMappingJobResult, KafkaSource}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryException}

import java.util.UUID
import java.util.concurrent.Executors
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import io.tofhir.engine.Execution.actorSystem
import io.tofhir.engine.data.write.SinkHandler
import io.tofhir.engine.execution.log.ExecutionLogger
import io.tofhir.engine.execution.processing.FileStreamInputArchiver
import io.tofhir.engine.model.exception.FhirMappingException
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
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

  // Keeps the scheduled jobs in the form of: jobId -> (executionId -> (Scheduler, execution))
  private val scheduledTasks: collection.mutable.Map[String, collection.mutable.Map[String, (Scheduler,FhirMappingJobExecution)]] =
    collection.mutable.Map[String, collection.mutable.Map[String, (Scheduler,FhirMappingJobExecution)]]()

  // Dedicated execution context for blocking streaming jobs
  private val streamingTaskExecutionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newCachedThreadPool)

  private val logger: Logger = Logger(this.getClass)

  /**
   * When the actor system is terminated i.e., the system is shutdown, log the status of running mapping jobs
   * as 'STOPPED' and scheduled mapping jobs as 'DESCHEDULED'.
   */
  actorSystem.whenTerminated
    .map(_ => {
      // iterate over all running tasks and log each one as 'STOPPED'
      runningTasks.values.flatMap(_.values)
        .foreach(execution => {
          // log execution status as 'STOPPED'
          ExecutionLogger.logExecutionStatus(execution, FhirMappingJobResult.STOPPED)
        })
      // iterate over all scheduled tasks and log each one as 'DESCHEDULED'
      scheduledTasks.values.flatMap(_.values.map(_._2))
        .foreach(execution => {
          // log execution status as 'DESCHEDULED'
          ExecutionLogger.logExecutionStatus(execution, FhirMappingJobResult.DESCHEDULED)
        })
    })

  /**
   * Caches a [[FhirMappingJobExecution]] for an individual mapping task
   *
   * @param execution            Execution containing the mapping tasks
   * @param mappingTaskName      Specific name which the [[StreamingQuery]] is associated to
   * @param streamingQueryFuture Future for the [[StreamingQuery]]
   * @return
   */
  def registerStreamingQuery(execution: FhirMappingJobExecution, mappingTaskName: String, streamingQueryFuture: Future[StreamingQuery]): Future[Unit] = {
    Future {
      // If there is an error in the streaming query execution, call 'stopMappingExecution' function,
      // which is responsible for removing it from the registry. Without that, the registry might contain incorrect
      // information such as indicating that the job is still running when it has encountered an error.
      streamingQueryFuture.recover(_ => stopMappingExecution(execution.jobId, execution.id, mappingTaskName))
      // Wait for the initial Future to be resolved
      val streamingQuery: StreamingQuery = Await.result(streamingQueryFuture, Duration.Inf)
      val jobId: String = execution.jobId
      val executionId: String = execution.id

      // Multiple threads can update the global task map. So, updates are synchronized.
      val updatedExecution = runningTasks.synchronized {
        // Update the execution map
        val executionMap: collection.mutable.Map[String, FhirMappingJobExecution] = runningTasks.getOrElseUpdate(jobId, collection.mutable.Map[String, FhirMappingJobExecution]())
        val updatedExecution: FhirMappingJobExecution = executionMap.get(executionId) match {
          case None => execution.copy(jobGroupIdOrStreamingQuery = Some(Right(collection.mutable.Map(mappingTaskName -> streamingQuery))))
          case Some(execution) => execution.copy(jobGroupIdOrStreamingQuery = Some(Right(execution.getStreamingQueryMap() + (mappingTaskName -> streamingQuery))))
        }
        executionMap.put(executionId, updatedExecution)
        updatedExecution
      }
      logger.debug(s"Streaming query for execution: $executionId, mappingTaskName: $mappingTaskName has been registered")

      try{
        // wait for StreamingQuery to terminate
        updatedExecution.getStreamingQuery(mappingTaskName).awaitTermination()
      }catch{
        case exception: StreamingQueryException =>
          Option(exception.getCause) match {
            case None =>
              ExecutionLogger.logExecutionStatus(execution, FhirMappingJobResult.FAILURE, Some(mappingTaskName), Some(exception), isChunkResult = false)
            case Some(cause) =>
              Option(cause.getCause) match {
                // special handling of UnknownTopicOrPartitionException to include the missing topic names
                case Some(_: UnknownTopicOrPartitionException) =>
                  val topicNames = execution.mappingTasks.find(mappingTask => mappingTask.name.contentEquals(mappingTaskName)).get.sourceBinding.map(source => source._2.asInstanceOf[KafkaSource].topicName).mkString(", ")
                  val unknownTopicError = FhirMappingException(s"The following Kafka topic(s) specified in the mapping task do not exist: $topicNames")
                  ExecutionLogger.logExecutionStatus(execution,FhirMappingJobResult.FAILURE,Some(mappingTaskName),Some(unknownTopicError),isChunkResult = false)
                case _ =>
                  ExecutionLogger.logExecutionStatus(execution,FhirMappingJobResult.FAILURE,Some(mappingTaskName),Some(exception),isChunkResult = false)
              }
          }
      }finally{
        // Remove the mapping execution from the running tasks after the query is terminated
        stopMappingExecution(jobId, executionId, mappingTaskName)
      }

      // Use the dedicated ExecutionContext for streaming jobs
    }(streamingTaskExecutionContext)
  }

  // TODO: Improve cancellation of running batch mapping jobs.
  //  Currently, we rely on setJobGroup and cancelJobGroup functions in Spark to cancel tasks associated with a mapping job.
  //  However, due to Spark's task distribution across multiple threads, using cancelJobGroup may not cancel all tasks,
  //  especially in the case of scheduled mapping jobs because setJobGroup only works for the Spark tasks started in the same thread.
  //  To resolve this issue, we need to assign a unique Spark job group id at the start of mapping job execution, before registering it with the RunningJobRegistry.
  //  We should call setJobGroup function before Spark tasks begin execution. The ideal location for this call seems to be the readSourceExecuteAndWriteInChunks function,
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
    val jobId: String = executionWithJobGroupId.jobId
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
      .getOrElseUpdate(mappingJobExecution.jobId, collection.mutable.Map[String, (Scheduler,FhirMappingJobExecution)]())
      .put(mappingJobExecution.id, (scheduler, mappingJobExecution))
    // log execution status as 'SCHEDULED'
    ExecutionLogger.logExecutionStatus(mappingJobExecution, FhirMappingJobResult.SCHEDULED)
    // add a scheduler listener to monitor task events
    scheduler.addSchedulerListener(new SchedulerListener {
      override def taskLaunching(executor: TaskExecutor): Unit = {
        registerBatchJob(mappingJobExecution,None,s"Spark job for job: ${mappingJobExecution.jobId} mappingTasks: ${mappingJobExecution.mappingTasks.map(_.name).mkString(" ")}")
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
    scheduledTasks(jobId)(executionId)._1.stop()
    logger.debug(s"Descheduled the mapping job with id: $jobId and execution: $executionId")
    // log execution status as 'DESCHEDULED'
    ExecutionLogger.logExecutionStatus(scheduledTasks(jobId)(executionId)._2, FhirMappingJobResult.DESCHEDULED)
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
              // Log each mapping task as stopped
              ExecutionLogger.logExecutionStatus(execution, FhirMappingJobResult.STOPPED, Some(queryEntry._1))
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
   * @param jobId            Identified of the associated with the execution
   * @param executionId      Identifier of the job containing the mapping
   * @param mappingTaskName  Name of the mappingTask
   */
  def stopMappingExecution(jobId: String, executionId: String, mappingTaskName: String): Unit = {
    removeMappingExecutionFromRunningTasks(jobId, executionId, mappingTaskName) match {
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
   * @param jobId           Identifier of the job
   * @param executionId     Identifier of the execution
   * @param mappingTaskName Name of the mappingTask
   * @return Returns the removed mapping entry if at all or None
   */
  private def removeMappingExecutionFromRunningTasks(jobId: String, executionId: String, mappingTaskName: String): Option[Either[String, StreamingQuery]] = {
    runningTasks.synchronized {
      runningTasks.get(jobId) match {
        case Some(jobMapping) if jobMapping.contains(executionId) =>
          val execution: FhirMappingJobExecution = jobMapping(executionId)
          var removedMappingEntry: Option[Either[String, StreamingQuery]] = None
          // If it is a batch job do nothing but warn user about the situation
          if (!execution.isStreamingJob) {
            logger.warn(s"Execution with $jobId: $jobId, executionId: $executionId, mappingTaskName: $mappingTaskName won't be stopped with a specific mapping as this is a batch job." +
              s"Stop execution by providing only the jobId and executionId")

            // Streaming query
          } else {
            if (execution.getStreamingQueryMap().contains(mappingTaskName)) {
              removedMappingEntry = Some(Right(execution.getStreamingQueryMap().remove(mappingTaskName).get))
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
   * @param jobId           Identifier of the job associated with the execution
   * @param executionId     Identifier of the execution to be stopped
   * @param mappingTaskName Name of the mappingTask representing the mapping task being executed
   * @return
   */
  def executionExists(jobId: String, executionId: String, mappingTaskName: Option[String]): Boolean = {
    if (runningTasks.contains(jobId) && runningTasks(jobId).contains(executionId)) {
      mappingTaskName match {
        case None => true // We know we have an execution at this point
        case Some(name) =>
          // For streaming jobs, we check whether there is a streaming query for the given mappingTask
          if (runningTasks(jobId)(executionId).isStreamingJob) {
            runningTasks(jobId)(executionId).getStreamingQueryMap().contains(name)

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
   * Gets scheduled executions for the given job
   *
   * @param jobId Identifier of the job
   * @return A set of execution ids
   */
  def getScheduledExecutions(jobId: String): Set[String] = {
    scheduledTasks.get(jobId).map(_.keySet).getOrElse(Set.empty).toSet
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
   * Converts the running task map into a structure as follows: (jobId -> sequence of (executionId -> sequence of mappingTask names))
   *
   * @return
   */
  def getRunningExecutions(): Map[String, Seq[(String, Seq[String])]] = {
      runningTasks.map(entry =>
        entry._1 -> // jobId
          entry._2 // a map in the form of (executionId -> FhirMappingJobExecution)
            .map(executionMappings => (executionMappings._1, executionMappings._2.mappingTasks.map(_.name))) // (executionId -> sequence of mappingTask names)
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
    removeExecutionFromRunningTasks(execution.jobId, execution.id)
  }
}
