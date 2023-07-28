package io.tofhir.server.model

import io.tofhir.engine.config.ErrorHandlingType
import io.tofhir.engine.config.ErrorHandlingType.ErrorHandlingType

/**
 * Execute task instance for getting mapping urls and error handling.
 *
 * @param mappingUrls           mapping urls for execute job
 * @param mappingErrorHandling  error handling type for execution
 */
case class ExecuteJobTask(mappingUrls: Option[Seq[String]] = None, mappingErrorHandling: Option[ErrorHandlingType])
