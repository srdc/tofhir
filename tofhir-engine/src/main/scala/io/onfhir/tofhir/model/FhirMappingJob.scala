package io.onfhir.tofhir.model

import io.onfhir.tofhir.config.MappingErrorHandling.MappingErrorHandling

/**
 * A mapping job including one or more mapping tasks from a configured data source to a configured sink
 * @param id                    Unique identifier for the mapping job
 * @param schedulingSettings    Scheduling information for periodic mapping jobs
 * @param sourceSettings        Settings for data source system(s)
 * @param sinkSettings          Settings for sink to write the mapped FHIR resources
 * @param mappings              Mapping tasks
 * @param mappingErrorHandling  Error handling methodology for mapping execution
 */
case class FhirMappingJob(id: String,
                          sourceSettings: Map[String,DataSourceSettings],
                          sinkSettings: FhirSinkSettings,
                          mappings: Seq[FhirMappingTask],
                          schedulingSettings: Option[SchedulingSettings] = None,
                          mappingErrorHandling: MappingErrorHandling
                         )

/**
 * Cron expression showing the times of scheduled task needed. More info: https://www.sauronsoftware.it/projects/cron4j/
 *
 * @param cronExpression A UNIX crontab-like pattern is a string split in five space separated parts
 * @param initialTime    If not specified, toFhir generates a time range between Java beginning (January 1, 1970) and next run time of task
 *                       If specified, toFhir generates a time range between initial time and next run time of task
 */
case class SchedulingSettings(cronExpression: String, initialTime: Option[String])
