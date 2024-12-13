package io.tofhir.engine.execution.log

import com.typesafe.scalalogging.Logger
import io.tofhir.engine.model.{FhirMappingJobExecution, FhirMappingJobResult}

/**
 * The ExecutionLogger is responsible for logging the execution status and results of mapping jobs.
 *
 * This object provides methods to:
 * - Log the explicit status of a mapping job execution (e.g., started, skipped, stopped, failed)
 * - Log the result of individual chunk executions for batch mapping jobs
 * - Log the result of mapping task executions for streaming jobs
 * - Log the overall result of a mapping task execution for batch mapping jobs after all chunks are completed
 */
object ExecutionLogger {
  private val logger: Logger = Logger(this.getClass)

  // Stores the overall result of the mapping task execution for batch mapping jobs
  // A mapping task execution is divided into multiple chunks based on the max chunk size configuration
  // Keeps active executions in the form of: executionId -> FhirMappingJobResult
  private val batchJobMappingTaskExecutionResults: collection.mutable.Map[String, FhirMappingJobResult] =
  collection.mutable.Map[String, FhirMappingJobResult]()

  /**
   * Logs the status of a mapping job execution explicitly, i.e., the status is determined by the application
   * itself, not the results of the mapping job execution.
   *
   * @param mappingJobExecution   The mapping job execution instance
   * @param status                The status to log (e.g., STARTED, SKIPPED, STOPPED, FAILED)
   * @param mappingTaskName       The optional name of the mapping
   * @param exception             The optional exception that occurred
   * @param isChunkResult         Indicate whether the log is result of a chunk
   */
  def logExecutionStatus(mappingJobExecution: FhirMappingJobExecution,
                         status: String,
                         mappingTaskName: Option[String] = None,
                         exception: Option[Throwable] = None,
                         isChunkResult: Boolean = true): Unit = {
    // create the job result
    val jobResult = FhirMappingJobResult(mappingJobExecution, mappingTaskName, status = Some(status), chunkResult = isChunkResult)
    // log the status with either info or error based on the presence of an exception
    exception match {
      case Some(e) => logger.error(jobResult.toMapMarker, jobResult.toString, e)
      case None => logger.info(jobResult.toMapMarker, jobResult.toString)
    }

    // update executions cache based on the status for batch jobs
    if (!mappingJobExecution.isStreamingJob) {
      status match {
        case FhirMappingJobResult.STARTED =>
          // clear the status, we will set it while logging the mapping task results
          batchJobMappingTaskExecutionResults.put(mappingJobExecution.id, jobResult.copy(status = None))
        case _ => // ignore other statuses
      }
    }
  }

  /**
   * Logs the result of an individual chunk execution for a batch mapping job.
   * A batch mapping job is divided into several chunks based on the given max chunk size configuration.
   *
   * @param mappingJobExecution The mapping job execution instance
   * @param mappingTaskName     The name of the mapping
   * @param numOfInvalids       The number of invalid records
   * @param numOfNotMapped      The number of records not mapped
   * @param numOfFhirResources  The number of FHIR resources created
   * @param numOfFailedWrites   The number of failed writes
   */
  def logExecutionResultForChunk(mappingJobExecution: FhirMappingJobExecution, mappingTaskName: String,
                                 numOfInvalids: Long = 0,
                                 numOfNotMapped: Long = 0,
                                 numOfFhirResources: Long = 0,
                                 numOfFailedWrites: Long = 0): Unit = {
    //Log the job result
    val jobResult = FhirMappingJobResult(mappingJobExecution, Some(mappingTaskName), numOfInvalids, numOfNotMapped, numOfFhirResources, numOfFailedWrites)
    logger.info(jobResult.toMapMarker, jobResult.toString)

    // modify the result of mapping job execution kept in the map
    val cachedResult = batchJobMappingTaskExecutionResults(mappingJobExecution.id)
    val updatedResult = cachedResult.copy(
      numOfNotMapped = cachedResult.numOfNotMapped + numOfNotMapped,
      numOfFailedWrites = cachedResult.numOfFailedWrites + numOfFailedWrites,
      numOfFhirResources = cachedResult.numOfFhirResources + numOfFhirResources,
      numOfInvalids = cachedResult.numOfInvalids + numOfInvalids
    )
    batchJobMappingTaskExecutionResults.put(mappingJobExecution.id, updatedResult)
  }

  /**
   * Logs the result of a mapping task execution for a streaming job.
   * A streaming job waits for data and executes the mapping task when the data arrives.
   *
   * @param mappingJobExecution The mapping job execution instance
   * @param mappingTaskName     The name of the mapping
   * @param numOfInvalids       The number of invalid records
   * @param numOfNotMapped      The number of records not mapped
   * @param numOfFhirResources  The number of FHIR resources created
   * @param numOfFailedWrites   The number of failed writes
   */
  def logExecutionResultForStreamingMappingTask(mappingJobExecution: FhirMappingJobExecution, mappingTaskName: String,
                                                numOfInvalids: Long = 0,
                                                numOfNotMapped: Long = 0,
                                                numOfFhirResources: Long = 0,
                                                numOfFailedWrites: Long = 0): Unit = {
    //Log the job result
    val jobResult = FhirMappingJobResult(mappingJobExecution, Some(mappingTaskName), numOfInvalids, numOfNotMapped, numOfFhirResources, numOfFailedWrites, chunkResult = false)
    logger.info(jobResult.toMapMarker, jobResult.toString)
  }

  /**
   * Logs the overall result of the execution of a mapping task for a batch mapping job once all chunks are completed.
   *
   * @param executionId The ID of the mapping job execution
   */
  def logExecutionResultForBatchMappingTask(executionId: String): Unit = {
    //Log the job result
    val jobResult = batchJobMappingTaskExecutionResults(executionId).copy(chunkResult = false)
    logger.info(jobResult.toMapMarker, jobResult.toString)
    // remove execution from the map
    batchJobMappingTaskExecutionResults.remove(executionId)
  }
}
