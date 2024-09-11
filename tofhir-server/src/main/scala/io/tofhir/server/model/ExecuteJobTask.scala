package io.tofhir.server.model


/**
 * The data transfer object to keep mapping names to be executed and corresponding mapping error handling.
 * @param mappingTaskNames           mappingTask names of the mappingTask's that to be executed
 */
case class ExecuteJobTask(clearCheckpoints: Boolean, mappingTaskNames: Option[Seq[String]] = None)
