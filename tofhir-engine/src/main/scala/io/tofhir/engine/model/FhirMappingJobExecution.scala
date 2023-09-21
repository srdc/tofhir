package io.tofhir.engine.model

import io.tofhir.engine.config.{ErrorHandlingType, ToFhirConfig}
import io.tofhir.engine.config.ErrorHandlingType.ErrorHandlingType

import java.util.UUID

/**
 * Represents the execution of mapping tasks included in a mapping job.
 *
 * @param id                   Unique identifier for the execution
 * @param projectId            Unique identifier of project to which mapping job belongs
 * @param job                  Fhir mapping job
 * @param mappingTasks         List of mapping tasks to be executed
 * @param mappingErrorHandling Error handling type for execution process
 */
case class FhirMappingJobExecution(id: String = UUID.randomUUID().toString,
                                   projectId: String = "",
                                   job: FhirMappingJob,
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
    s"./checkpoint/${job.id}/${mappingUrl.hashCode}"

  /**
   * Creates a error output directory for a mapping execution included in a job and an execution
   * error-folder-path\<error-type>\job-<jobId>\execution-<executionId>\<hashedMappingUrl>\<random-generated-name-by-spark>.csv
   * e.g. error-folder-path\invalid_input\job-d13b5044-f05c-4698-86c2-d83b3c5083f8\execution-59733de5-1c92-4741-b032-6e9e13ee4550\-521848504\part-00000-1d7d9467-0195-4d28-964d-89171727fa41-c000.csv
   *
   * @param mappingUrl
   * @param errorType
   * @return
   */
  def getErrorOutputDirectory(mappingUrl: String, errorType: String): String =
    s"${ToFhirConfig.engineConfig.erroneousRecordsFolder}/${errorType}/job-${job.id}/execution-${id}/${mappingUrl.hashCode}"

}
