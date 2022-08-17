package io.onfhir.tofhir.engine

import it.sauronsoftware.cron4j.Scheduler

import java.net.URI

/**
 * Holds a Scheduler and a URI to a folder in which latest synchronization times are kept. That folders acts
 * as a database.
 *
 * @param scheduler
 * @param folderUri
 */
case class MappingJobScheduler(scheduler: Scheduler, folderUri: URI) {

}
