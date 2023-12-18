package io.tofhir.server.model


/**
 * The data transfer object to keep mapping urls to be executed and corresponding mapping error handling.
 * @param mappingUrls           mapping urls of the mappingTask's that to be executed
 */
case class ExecuteJobTask(clearCheckpoints: Boolean, mappingUrls: Option[Seq[String]] = None)
