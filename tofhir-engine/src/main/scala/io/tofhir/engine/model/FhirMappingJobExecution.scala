package io.tofhir.engine.model

import io.tofhir.engine.config.ErrorHandlingType
import io.tofhir.engine.config.ErrorHandlingType.ErrorHandlingType

import java.util.UUID

/**
 * Represents the execution of mapping tasks included in a mapping job.
 *
 * @param id                   Unique identifier for the execution
 * @param projectId            Unique identifier of project to which mapping job belongs
 * @param jobId                Unique identifier of mapping job
 * @param mappingTasks         List of mapping tasks to be executed
 * @param mappingErrorHandling Error handling type for execution process
 */
case class FhirMappingJobExecution(id: String = UUID.randomUUID().toString,
                                   projectId: String = "",
                                   jobId: String = UUID.randomUUID().toString,
                                   mappingTasks: Seq[FhirMappingTask] = Seq.empty,
                                   mappingErrorHandling: ErrorHandlingType = ErrorHandlingType.CONTINUE
                                  ) {
  /**
   * Creates a checkpoint directory for a mapping included in a job
   *
   * @param mappingUrl Url of the mapping
   * @return Directory path in which the checkpoints will be managed
   */
  def getCheckpointDirectory(mappingUrl: String): String =
    s"./checkpoint/$jobId/${mappingUrl.hashCode}"
}
