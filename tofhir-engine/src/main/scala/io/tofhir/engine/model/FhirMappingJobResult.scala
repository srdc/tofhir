package io.tofhir.engine.model

import ch.qos.logback.more.appenders.marker.MapMarker
import io.tofhir.engine.util.TimeUtil
/**
 * Result of a mapping job execution
 *
 * @param mappingJobExecution Fhir Mapping Job execution
 * @param mappingTaskName     Name of the mapping if this is reporting single mapping
 * @param numOfInvalids       Total number of invalid input rows
 * @param numOfNotMapped      Total number of mappings per row that is not successful
 * @param numOfFhirResources  Total number of FHIR resources created as a result mapping(s)
 * @param numOfFailedWrites   Total number of FHIR resources that cannot be written to the configured sink (e.g. FHIR repository)
 * @param status              An optional status indicating the overall outcome of the mapping job.
 * @param chunkResult         Whether it represents the result of a chunk (applicable only for the batch mapping job) or the execution of a mapping task
 * @param totalNumOfChunks    Total number of chunks for the batch mapping job execution
 * @param completedNumOfChunks Number of chunks that have been completed so far in the batch mapping job execution
 */
case class FhirMappingJobResult(mappingJobExecution: FhirMappingJobExecution,
                                mappingTaskName: Option[String],
                                numOfInvalids: Long = 0,
                                numOfNotMapped: Long = 0,
                                numOfFhirResources: Long = 0,
                                numOfFailedWrites: Long = 0,
                                status: Option[String] = None,
                                chunkResult: Boolean = true,
                                totalNumOfChunks: Int = 1,
                                completedNumOfChunks: Int = 0
                               ) {
  final val eventId: String = "MAPPING_JOB_RESULT"
  /**
   * Determines the status of a FHIR mapping job. If it has an associated status, it is returned.
   * Otherwise, the status is determined using the result of the mapping job i.e. the number of
   * invalid rows, the number of failed writes etc.
   */
  val result: String = {
    if(status.nonEmpty)
      status.get
    else
      numOfInvalids + numOfNotMapped + numOfFailedWrites match {
        case 0 => FhirMappingJobResult.SUCCESS
        case _ => // Check if it is complete failure or there are some successfully written resources
          numOfFhirResources match {
            case 0 => FhirMappingJobResult.FAILURE
            case _ => FhirMappingJobResult.PARTIAL_SUCCESS
          }
      }
  }

  override def toString: String = {
    // get the status of execution
    val status: String = result
    // construct the message for the execution of mapping job
    var message = s"toFHIR ${if (chunkResult) "chunk " else "" }mapping result ($status) for execution '${mappingJobExecution.id}' of job '${mappingJobExecution.jobId}' in project '${mappingJobExecution.projectId}'${mappingTaskName.map(u => s" for mappingTask '$u'").getOrElse("")}!\n"
    // if it is not the start log, print the result of execution
    if(!status.contentEquals(FhirMappingJobResult.STARTED)){
      message = message +
        s"\t# of Invalid Rows: \t$numOfInvalids\n" +
        s"\t# of Not Mapped: \t$numOfNotMapped\n" +
        s"\t# of Failed writes:\t$numOfFailedWrites\n" +
        s"\t# of Written FHIR resources:\t$numOfFhirResources"
    }
    message
  }

  /**
   * Converts the mapping job execution to a MapMarker object.
   *
   * @return The MapMarker representing the mapping job execution.
   */
  def toMapMarker: MapMarker = {
    // create a new HashMap to store the marker attributes
    val markerMap: java.util.Map[String, Any] = new java.util.HashMap[String, Any]()
    // add attributes to the marker map
    markerMap.put("jobId", mappingJobExecution.jobId)
    markerMap.put("projectId", mappingJobExecution.projectId)
    markerMap.put("executionId", mappingJobExecution.id)
    markerMap.put("mappingTaskName", mappingTaskName.orNull)
    markerMap.put("chunkResult", chunkResult)
    // Store the overall execution result if chunkResult is null or absent.
    // Otherwise, mark the result as "STARTED" to indicate an ongoing process.
    if(!chunkResult){
      markerMap.put("result", result)
    } else {
      markerMap.put("result", FhirMappingJobResult.STARTED)
    }
    markerMap.put("numOfInvalids", numOfInvalids)
    markerMap.put("numOfNotMapped", numOfNotMapped)
    markerMap.put("numOfFhirResources", numOfFhirResources)
    markerMap.put("numOfFailedWrites", numOfFailedWrites)
    markerMap.put("eventId", eventId)
    markerMap.put("isStreamingJob", mappingJobExecution.isStreamingJob)
    markerMap.put("isScheduledJob", mappingJobExecution.isScheduledJob)
    // log the chunk progress for batch jobs
    if(!mappingJobExecution.isStreamingJob){
      markerMap.put("chunkProgress",s"$completedNumOfChunks / $totalNumOfChunks")
    }
    // The current timestamp is automatically added to the log entry when it is sent to Elasticsearch or written to a file.
    // As a result, there is no need to manually add a "@timestamp" field.
    // However, during the process of writing the log to Elasticsearch, the timestamp is rounded, resulting in a loss of precision.
    // For example, "2024-08-28_13:54:44.740" may be rounded to "2024-08-28_13:54:44.000" in Elasticsearch.
    // This rounding leads to the loss of crucial millisecond information, which is important for accurately sorting logs.
    markerMap.put("@timestamp", TimeUtil.getCurrentISOTime)
    // create a new MapMarker using the marker map
    new MapMarker("marker", markerMap)
  }
}

/**
 * Object representing possible statuses of a FHIR mapping job.
 */
object FhirMappingJobResult {
  /**
   * Represents the status when the mapping job has been started.
   * Resources are in the process of being mapped and written.
   */
  val STARTED: String = "STARTED"

  /**
   * Represents the status when the mapping job has failed.
   * No FHIR Resources are written because the mapping or writing process has encountered a failure.
   */
  val FAILURE: String = "FAILURE"

  /**
   * Represents the status when the mapping job has achieved partial success.
   * Some of the FHIR Resources are written to the configured FHIR Repository or File System,
   * while some have encountered failures during the process.
   */
  val PARTIAL_SUCCESS: String = "PARTIAL_SUCCESS"

  /**
   * Represents the status when the mapping job has successfully completed.
   * All FHIR Resources are written to the configured FHIR Repository or File System.
   */
  val SUCCESS: String = "SUCCESS"

  /**
   * Represents the status when the mapping task has been skipped.
   * When a batch mapping job is stopped, the remaining mapping tasks i.e., the ones that are not yet executed, are marked as SKIPPED.
   * These tasks are not processed further and are considered skipped due to the premature termination of the job.
   */
  val SKIPPED: String = "SKIPPED"

  /**
   * Represents the status when the mapping task has been stopped.
   * This status indicates that the execution of the mapping task has been manually stopped before completion.
   * It allows distinguishing between tasks that were intentionally halted and those that failed.
   */
  val STOPPED: String = "STOPPED"

  /**
   * Represents the status when the mapping job has been scheduled.
   * This status indicates that the mapping job is planned to be executed at a later time.
   */
  val SCHEDULED: String = "SCHEDULED"

  /**
   * Represents the status when the mapping job has been descheduled.
   * This status indicates that the previously scheduled mapping job has been canceled and will not be executed.
   */
  val DESCHEDULED: String = "DESCHEDULED"
}