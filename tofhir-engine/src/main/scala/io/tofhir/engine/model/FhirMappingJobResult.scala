package io.tofhir.engine.model

import net.logstash.logback.marker.LogstashMarker
import net.logstash.logback.marker.Markers.{append}

/**
 * Result of a batch mapping job execution
 *
 * @param mappingJobExecution Fhir Mapping Job execution
 * @param mappingUrl          Url of the mapping if this is reporting single mapping
 * @param numOfInvalids       Total number of invalid input rows
 * @param numOfNotMapped      Total number of mappings per row that is not successful
 * @param numOfFhirResources  Total number of FHIR resources created as a result mapping(s)
 * @param numOfFailedWrites   Total number of FHIR resources that cannot be written to the configured sink (e.g. FHIR repository)
 * @param status              An optional status indicating the overall outcome of the mapping job.
 */
case class FhirMappingJobResult(mappingJobExecution: FhirMappingJobExecution,
                                mappingUrl: Option[String],
                                numOfInvalids: Long = -1,
                                numOfNotMapped: Long = -1,
                                numOfFhirResources: Long = -1,
                                numOfFailedWrites: Long = -1,
                                status: Option[String] = None
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
        case -3 => FhirMappingJobResult.STARTED
        case 0 => FhirMappingJobResult.SUCCESS
        case _ => // Check if it is complete failure or there are some successfully written resources
          numOfFhirResources match {
            case 0 => FhirMappingJobResult.FAILURE
            case _ => FhirMappingJobResult.PARTIAL_SUCCESS
          }
      }
  }

  override def toString: String = {
    s"toFHIR batch mapping result ($result) for execution '${mappingJobExecution.id}' of job '${mappingJobExecution.job.id}' in project '${mappingJobExecution.projectId}'${mappingUrl.map(u => s" for mapping '$u'").getOrElse("")}!\n" +
        s"\t# of Invalid Rows: \t$numOfInvalids\n" +
          s"\t# of Not Mapped: \t$numOfNotMapped\n" +
          s"\t# of Failed writes:\t$numOfFailedWrites\n" +
          s"\t# of Written FHIR resources:\t$numOfFhirResources"
  }

  /**
   *
   * @return
   */
  def toLogstashMarker: LogstashMarker = {
    append("jobId", mappingJobExecution.job.id)
      .and(append("projectId", mappingJobExecution.projectId)
        .and(append("executionId", mappingJobExecution.id)
          .and(append("mappingUrl", mappingUrl.orElse(null))
            .and(append("result", result)
              .and(append("numOfInvalids", numOfInvalids)
                .and(append("numOfNotMapped", numOfNotMapped)
                  .and(append("numOfFhirResources", numOfFhirResources)
                    .and(append("numOfFailedWrites", numOfFailedWrites)
                      .and(append("eventId", eventId))))))))))
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
}