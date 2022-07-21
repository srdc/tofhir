package io.onfhir.tofhir.model

import io.onfhir.tofhir.config.MappingErrorHandling.MappingErrorHandling

/**
 * A mapping job including one or more mapping tasks from a configured data source to a configured sink
 * @param id                          Unique identifier for the mapping job
 * @param sourceSettings              Settings for data source system(s)
 * @param sinkSettings                Settings for sink to write the mapped FHIR resources
 * @param terminologyServiceSettings  Settings for a terminology service that will be used within mappings
 * @param identityServiceSettings     Settings for a external identity service that will be used within mappings
 * @param mappings                    Mapping tasks
 * @param schedulingSettings          Scheduling information for periodic mapping jobs
 * @param mappingErrorHandling        Error handling methodology for mapping execution
 * @param useFhirSinkAsIdentityService  If true it means the FHIR repository to write the mapped resources where the configuration
 *                                      is given in sink settings will be used as identity service (Override identityServiceSettings if given)
 */
case class FhirMappingJob(id: String,
                          sourceSettings: Map[String,DataSourceSettings],
                          sinkSettings: FhirSinkSettings,
                          terminologyServiceSettings:Option[TerminologyServiceSettings] = None,
                          identityServiceSettings: Option[IdentityServiceSettings] = None,
                          mappings: Seq[FhirMappingTask],
                          schedulingSettings: Option[SchedulingSettings] = None,
                          mappingErrorHandling: MappingErrorHandling,
                          useFhirSinkAsIdentityService:Boolean = false
                         ) {
  /**
   * Return the final identity service settings
   * @return
   */
  def getIdentityServiceSettings():Option[IdentityServiceSettings] =
    if(useFhirSinkAsIdentityService &&sinkSettings.isInstanceOf[FhirRepositorySinkSettings])
      Some(sinkSettings.asInstanceOf[FhirRepositorySinkSettings])
    else identityServiceSettings
}

/**
 * Cron expression showing the times of scheduled task needed. More info: https://www.sauronsoftware.it/projects/cron4j/
 *
 * @param cronExpression A UNIX crontab-like pattern is a string split in five space separated parts
 * @param initialTime    If not specified, toFhir generates a time range between Java beginning (January 1, 1970) and next run time of task
 *                       If specified, toFhir generates a time range between initial time and next run time of task
 */
case class SchedulingSettings(cronExpression: String, initialTime: Option[String])
