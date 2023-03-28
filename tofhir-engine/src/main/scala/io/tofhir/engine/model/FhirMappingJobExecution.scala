package io.tofhir.engine.model

import java.util.UUID

/**
 * Represents the execution of mapping tasks included in a mapping job.
 *
 * @param id           Unique identifier for the execution
 * @param projectId    Unique identifier of project to which mapping job belongs
 * @param jobId        Unique identifier of mapping job
 * @param mappingTasks List of mapping tasks to be executed
 */
case class FhirMappingJobExecution(id: String = UUID.randomUUID().toString,
                                   projectId: String = "",
                                   jobId: String = UUID.randomUUID().toString,
                                   mappingTasks: Seq[FhirMappingTask] = Seq.empty
                                  )