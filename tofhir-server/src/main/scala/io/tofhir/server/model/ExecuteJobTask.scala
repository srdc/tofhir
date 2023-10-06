package io.tofhir.server.model

import io.tofhir.engine.config.ErrorHandlingType.ErrorHandlingType

/**
 * The data transfer object to keep mapping urls to be executed and corresponding mapping error handling.
 * @param mappingUrls           mapping urls of the mappingTask's that to be executed
 * @param mappingErrorHandling  error handling type for this execution
 */
case class ExecuteJobTask(clearCheckpoints: Boolean, mappingUrls: Option[Seq[String]] = None, mappingErrorHandling: Option[ErrorHandlingType])
