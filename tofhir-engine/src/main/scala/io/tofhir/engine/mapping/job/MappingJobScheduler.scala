package io.tofhir.engine.mapping.job

import it.sauronsoftware.cron4j.Scheduler

import java.net.URI
import java.nio.file.Paths

/**
 * Holds a Scheduler and a URI to a folder in which latest synchronization times are kept. That folders acts
 * as a database.
 *
 * @param scheduler
 * @param folderUri
 */
case class MappingJobScheduler(scheduler: Scheduler, folderUri: URI)

object MappingJobScheduler {
  /**
   * Creates an instance of MappingJobScheduler with the provided folder path.
   *
   * @param toFhirDbFolderPath The folder path for database
   * @return An instance of MappingJobScheduler.
   * @throws IllegalArgumentException if toFhirDbFolderPath is empty.
   */
  def instance(toFhirDbFolderPath:String): MappingJobScheduler = {
    if (toFhirDbFolderPath.isEmpty) {
      throw new IllegalArgumentException("runJob is called with a scheduled mapping job, but toFhir.db is not configured.");
    }
    MappingJobScheduler(new Scheduler(), Paths.get(toFhirDbFolderPath, "scheduler").toUri)
  }
}
