package io.tofhir.engine.model

import io.tofhir.engine.util.FhirMappingJobFormatter
import org.json4s.JsonAST.{JObject, JString}

import java.util.UUID
import javax.ws.rs.BadRequestException

/**
 * A mapping job including one or more mapping tasks from a configured data source to a configured sink
 *
 * @param id                           Unique identifier for the mapping job
 * @param name                         Name of the mapping job
 * @param description                  Description of the mapping job
 * @param sourceSettings               Settings for data source system(s)
 * @param sinkSettings                 Settings for sink to write the mapped FHIR resources
 * @param terminologyServiceSettings   Settings for a terminology service that will be used within mappings
 * @param identityServiceSettings      Settings for a external identity service that will be used within mappings
 * @param mappings                     Mapping tasks
 * @param schedulingSettings           Scheduling information for periodic mapping jobs
 * @param dataProcessingSettings       Settings for data processing(e.g. archiveMode etc.)
 * @param useFhirSinkAsIdentityService If true it means the FHIR repository to write the mapped resources where the configuration
 *                                     is given in sink settings will be used as identity service (Override identityServiceSettings if given)
 */
case class FhirMappingJob(id: String = UUID.randomUUID().toString,
                          name: Option[String] = None,
                          description: Option[String] = None,
                          sourceSettings: Map[String, MappingJobSourceSettings],
                          sinkSettings: FhirSinkSettings,
                          terminologyServiceSettings: Option[TerminologyServiceSettings] = None,
                          identityServiceSettings: Option[IdentityServiceSettings] = None,
                          mappings: Seq[FhirMappingTask],
                          schedulingSettings: Option[BaseSchedulingSettings] = None,
                          dataProcessingSettings: DataProcessingSettings = DataProcessingSettings(),
                          useFhirSinkAsIdentityService: Boolean = false
                         ) {
  /**
   * Validates the mapping job
   *
   * @throws BadRequestException if a streaming job is attempted to be scheduled or if a data source referenced
   *                             by a mapping task is missing from the source settings.
   */
  def validate(): Unit = {
    if (sourceSettings.exists(_._2.asStream) && schedulingSettings.nonEmpty) {
      throw new BadRequestException("Streaming jobs cannot be scheduled.")
    }

    // Check names of the mappingTasks, if a duplicate name is found, throw an error
    val duplicateMappingTasks = FhirMappingJobFormatter.findDuplicateMappingTaskNames(mappings)
    if (duplicateMappingTasks.nonEmpty) {
      throw new BadRequestException(s"Duplicate MappingTask name(s) found: ${duplicateMappingTasks.mkString(", ")}. Each MappingTask must have a unique name.")
    }

    // Check mapping tasks of the job, if a data source of a mapping task is missing, throw an error
    mappings.foreach(mappingTask => {
      mappingTask.sourceBinding.foreach(sourceBinding => {
        if (sourceBinding._2.sourceRef.nonEmpty && !sourceSettings.contains(sourceBinding._2.sourceRef.get)) {
          throw new BadRequestException(s"The data source referenced by source name '${sourceBinding._2.sourceRef.get}' in the mapping task '${mappingTask.name}' is not found in the source settings of the job.")
        }
      })
    })
  }

  /**
   * Return the final identity service settings
   *
   * @return
   */
  def getIdentityServiceSettings(): Option[IdentityServiceSettings] =
    if (useFhirSinkAsIdentityService && sinkSettings.isInstanceOf[FhirRepositorySinkSettings])
      Some(sinkSettings.asInstanceOf[FhirRepositorySinkSettings])
    else identityServiceSettings

  /**
   * Retrieves metadata for this mapping job. The metadata is being used in the folder-based implementation for the time being.
   *
   * @return
   */
  def getMetadata(): JObject = {
    JObject(
      List(
        "id" -> JString(this.id),
        "name" -> JString(this.name.getOrElse(""))
      )
    )
  }
}

/**
 * Interface defining scheduling settings for mapping jobs.
 */
trait BaseSchedulingSettings {
  /**
   * Specifies a UNIX crontab-like pattern split into five space-separated parts.
   * For more details, refer to: https://www.sauronsoftware.it/projects/cron4j/
   */
  val cronExpression: String
}

/**
 * Represents scheduling settings for mapping jobs.
 *
 * @param cronExpression A UNIX crontab-like pattern split into five space-separated parts.
 */
case class SchedulingSettings(cronExpression: String) extends BaseSchedulingSettings

/**
 * Represents scheduling settings for SQL data sources.
 *
 * @param cronExpression A UNIX crontab-like pattern split into five space-separated parts.
 * @param initialTime    If not specified, toFhir generates a time range between Java beginning (January 1, 1970) and next run time of task
 *                       If specified, toFhir generates a time range between initial time and next run time of task
 */
case class SQLSchedulingSettings(cronExpression: String, initialTime: Option[String]) extends BaseSchedulingSettings
