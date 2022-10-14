package io.tofhir.engine.mapping

import io.tofhir.engine.model.{DataSourceSettings, FhirMappingResult, FhirMappingTask, FhirSinkSettings, IdentityServiceSettings, SchedulingSettings, TerminologyServiceSettings}
import org.apache.spark.sql.streaming.StreamingQuery

import java.time.LocalDateTime
import java.util.UUID
import scala.concurrent.Future

/**
 * Interface to submit mapping jobs to tofhir
 */
trait IFhirMappingJobManager {

  /**
   * Execute the given mapping job and write the resulting FHIR resources to the given sink
   *
   * @param id                         Unique job identifier
   * @param tasks                      Mapping tasks that will be executed in sequential
   * @param sourceSettings             Settings for the source system(s)
   * @param sinkSettings               FHIR sink settings (can be a FHIR repository, file system, kafka)
   * @param terminologyServiceSettings Settings for terminology service to use within mappings (e.g. lookupDisplay)
   * @param identityServiceSettings    Settings for identity service to use within mappings (e.g. resolveIdentifier)
   * @param timeRange                  If given, execute the mapping job for data between the given interval
   * @return
   */
  def executeMappingJob(id: String = UUID.randomUUID().toString,
                        tasks: Seq[FhirMappingTask],
                        sourceSettings: Map[String, DataSourceSettings],
                        sinkSettings: FhirSinkSettings,
                        terminologyServiceSettings: Option[TerminologyServiceSettings] = None,
                        identityServiceSettings: Option[IdentityServiceSettings] = None,
                        timeRange: Option[(LocalDateTime, LocalDateTime)] = None): Future[Unit]

  /**
   * Start streaming mapping job
   *
   * @param id                         Job identifier
   * @param tasks                      Mapping tasks that will be executed in parallel in stream mode
   * @param sourceSettings             Settings for the source system(s)
   * @param sinkSettings               FHIR sink settings (can be a FHIR repository, file system, kafka)
   * @param terminologyServiceSettings Settings for terminology service to use within mappings (e.g. lookupDisplay)
   * @param identityServiceSettings    Settings for identity service to use within mappings (e.g. resolveIdentifier)
   * @return
   */
  def startMappingJobStream(id: String = UUID.randomUUID().toString,
                            tasks: Seq[FhirMappingTask],
                            sourceSettings: Map[String, DataSourceSettings],
                            sinkSettings: FhirSinkSettings,
                            terminologyServiceSettings: Option[TerminologyServiceSettings] = None,
                            identityServiceSettings: Option[IdentityServiceSettings] = None,
                           ): StreamingQuery

  /**
   * Schedule to execute the given mapping job with given cron expression and write the resulting FHIR resources to the given sink
   *
   * @param id                         Job identifier
   * @param tasks                      Mapping tasks that will be scheduled
   * @param sourceSettings             Settings for the source system(s)
   * @param sinkSettings               FHIR sink settings (can be a FHIR repository, file system, kafka)
   * @param schedulingSettings         Scheduling info
   * @param terminologyServiceSettings Settings for terminology service to use within mappings (e.g. lookupDisplay)
   * @param identityServiceSettings    Settings for identity service to use within mappings (e.g. resolveIdentifier)
   * @return
   */

  def scheduleMappingJob(id: String = UUID.randomUUID().toString,
                         tasks: Seq[FhirMappingTask],
                         sourceSettings: Map[String, DataSourceSettings],
                         sinkSettings: FhirSinkSettings,
                         schedulingSettings: SchedulingSettings,
                         terminologyServiceSettings: Option[TerminologyServiceSettings] = None,
                         identityServiceSettings: Option[IdentityServiceSettings] = None): Unit

  /**
   * Execute the given mapping task and write the resulting FHIR resources to the given sink
   *
   * @param id                         Unique job identifier
   * @param task                       A Mapping task that will be executed
   * @param sourceSettings             Settings for the source system(s)
   * @param sinkSettings               FHIR sink settings (can be a FHIR repository, file system, kafka)
   * @param terminologyServiceSettings Settings for terminology service to use within mappings (e.g. lookupDisplay)
   * @param identityServiceSettings    Settings for identity service to use within mappings (e.g. resolveIdentifier)
   * @return
   */
  def executeMappingTask(id: String = UUID.randomUUID().toString,
                         task: FhirMappingTask,
                         sourceSettings: Map[String, DataSourceSettings],
                         sinkSettings: FhirSinkSettings,
                         terminologyServiceSettings: Option[TerminologyServiceSettings] = None,
                         identityServiceSettings: Option[IdentityServiceSettings] = None
                        ): Future[Unit]

  /**
   * Execute the given mapping task and return the resulting FhirMappingResult
   *
   * @param id                         Unique job identifier
   * @param task                       Mapping task that will be executed
   * @param sourceSettings             Settings for the source system(s)
   * @param terminologyServiceSettings Settings for terminology service to use within mappings (e.g. lookupDisplay)
   * @param identityServiceSettings    Settings for identity service to use within mappings (e.g. resolveIdentifier)
   * @return
   */
  def executeMappingTaskAndReturn(id: String = UUID.randomUUID().toString,
                                  task: FhirMappingTask,
                                  sourceSettings: Map[String, DataSourceSettings],
                                  terminologyServiceSettings: Option[TerminologyServiceSettings] = None,
                                  identityServiceSettings: Option[IdentityServiceSettings] = None
                                 ): Future[Seq[FhirMappingResult]]
}
