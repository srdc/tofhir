package io.tofhir.engine.model

import net.logstash.logback.marker.LogstashMarker
import net.logstash.logback.marker.Markers.{append}

/**
 * Result of a mapping job for a batch of input
 * @param mappingJobExecution    Fhir Mapping Job execution
 * @param mappingUrl             Url of the mapping if this is reporting single mapping
 * @param numOfInvalids          Total number of invalid input rows
 * @param numOfNotMapped         Total number of mappings per row that is not successful
 * @param numOfFhirResources     Total number of FHIR resources created as a result mapping(s)
 * @param numOfFailedWrites      Total number of FHIR resources that cannot be written to the configured sink (e.g. FHIR repository)
 */
case class FhirMappingJobResult(mappingJobExecution: FhirMappingJobExecution,
                                mappingUrl:Option[String],
                                numOfInvalids:Long = -1,
                                numOfNotMapped:Long = -1,
                                numOfFhirResources:Long = -1,
                                numOfFailedWrites:Long = -1
                               ) {
  final val eventId:String = "MAPPING_JOB_RESULT"
  val  result:String = (numOfInvalids + numOfNotMapped + numOfFailedWrites) match {
    case 0 => "SUCCESS"
    case neg if neg < 0 => "FAILURE"
    case _ => "PARTIAL_SUCCESS"
  }

  override def toString: String = {
    s"toFHIR batch mapping result ($result) for execution '${mappingJobExecution.id}' of job '${mappingJobExecution.jobId}' in project '${mappingJobExecution.projectId}'${mappingUrl.map(u => s" for mapping '$u'").getOrElse("")}!\n" +
      (if(result != "FAILURE")
        s"\t# of Invalid Rows: \t$numOfInvalids\n" +
        s"\t# of Not Mapped: \t$numOfNotMapped\n" +
        s"\t# of FHIR resources:\t$numOfFhirResources\n" +
        s"\t# of Failed writes:\t$numOfFailedWrites"
      else
        ""
        )
  }

  /**
   *
   * @return
   */
  def toLogstashMarker: LogstashMarker = {
    append("jobId", mappingJobExecution.jobId)
      .and(append("projectId",mappingJobExecution.projectId)
      .and(append("executionId",mappingJobExecution.id)
      .and(append("mappingUrl", mappingUrl.orElse(null))
        .and(append("result", result)
          .and(append("numOfInvalids", numOfInvalids)
          .and(append("numOfNotMapped", numOfNotMapped)
          .and(append("numOfFhirResources", numOfFhirResources)
            .and(append("numOfFailedWrites", numOfFailedWrites)
              .and(append("eventId", eventId))))))))))
  }
}
