package io.onfhir.tofhir.engine

import io.onfhir.tofhir.model.{FhirMappingTask, FhirSinkSettings}
import org.apache.spark.sql.streaming.StreamingQuery

import java.util.UUID

trait IFhirMappingStreamingJobManager {

  /**
   * Start streaming mapping job
   * @param id            Job identifier
   * @param tasks         Mapping tasks that will be executed in parallel in stream mode
   * @param sinkSettings  FHIR sink settings (can be a FHIR repository, file system, kafka)
   * @return
   */
  def executeMappingJob(id: String = UUID.randomUUID().toString,
                        tasks: Seq[FhirMappingTask],
                        sinkSettings: FhirSinkSettings):StreamingQuery

}
