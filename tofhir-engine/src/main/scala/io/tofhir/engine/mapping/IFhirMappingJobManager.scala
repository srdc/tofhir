package io.tofhir.engine.mapping

import java.time.LocalDateTime

import io.tofhir.engine.model._
import org.apache.spark.sql.streaming.StreamingQuery

import scala.concurrent.Future

/**
 * Interface to submit mapping jobs to tofhir
 */
trait IFhirMappingJobManager {

  /**
   * Execute the given mapping job and write the resulting FHIR resources to the given sink
   *
   * @param mappingJobExecution        Fhir Mapping Job execution
   * @param sourceSettings             Settings for the source system(s)
   * @param sinkSettings               FHIR sink settings (can be a FHIR repository, file system, kafka)
   * @param terminologyServiceSettings Settings for terminology service to use within mappings (e.g. lookupDisplay)
   * @param identityServiceSettings    Settings for identity service to use within mappings (e.g. resolveIdentifier)
   * @param timeRange                  If given, execute the mapping job for data between the given interval
   * @return
   */
  def executeMappingJob(mappingJobExecution:FhirMappingJobExecution,
                        sourceSettings: Map[String, DataSourceSettings],
                        sinkSettings: FhirSinkSettings,
                        terminologyServiceSettings: Option[TerminologyServiceSettings] = None,
                        identityServiceSettings: Option[IdentityServiceSettings] = None,
                        timeRange: Option[(LocalDateTime, LocalDateTime)] = None): Future[Unit]

  /**
   * Start streaming mapping job. A [[Future]] of [[StreamingQuery]] is returned for each mapping task included in the job, encapsulated in a map.
   *
   * @param mappingJobExecution        Fhir Mapping Job execution
   * @param sourceSettings             Settings for the source system(s)
   * @param sinkSettings               FHIR sink settings (can be a FHIR repository, file system, kafka)
   * @param terminologyServiceSettings Settings for terminology service to use within mappings (e.g. lookupDisplay)
   * @param identityServiceSettings    Settings for identity service to use within mappings (e.g. resolveIdentifier)
   * @return A map of (mapping url -> streaming query futures).
   */
  def startMappingJobStream(mappingJobExecution:FhirMappingJobExecution,
                            sourceSettings: Map[String, DataSourceSettings],
                            sinkSettings: FhirSinkSettings,
                            terminologyServiceSettings: Option[TerminologyServiceSettings] = None,
                            identityServiceSettings: Option[IdentityServiceSettings] = None,
                           ): Map[String, Future[StreamingQuery]]

  /**
   * Schedule to execute the given mapping job with given cron expression and write the resulting FHIR resources to the given sink
   *
   * @param mappingJobExecution        Fhir Mapping Job execution
   * @param sourceSettings             Settings for the source system(s)
   * @param sinkSettings               FHIR sink settings (can be a FHIR repository, file system, kafka)
   * @param schedulingSettings         Scheduling info
   * @param terminologyServiceSettings Settings for terminology service to use within mappings (e.g. lookupDisplay)
   * @param identityServiceSettings    Settings for identity service to use within mappings (e.g. resolveIdentifier)
   * @return
   */

  def scheduleMappingJob(mappingJobExecution:FhirMappingJobExecution,
                         sourceSettings: Map[String, DataSourceSettings],
                         sinkSettings: FhirSinkSettings,
                         schedulingSettings: BaseSchedulingSettings,
                         terminologyServiceSettings: Option[TerminologyServiceSettings] = None,
                         identityServiceSettings: Option[IdentityServiceSettings] = None): Unit

  /**
   * Execute the given mapping task and write the resulting FHIR resources to the given sink
   *
   * @param mappingJobExecution        Fhir Mapping Job execution
   * @param sourceSettings             Settings for the source system(s)
   * @param sinkSettings               FHIR sink settings (can be a FHIR repository, file system, kafka)
   * @param terminologyServiceSettings Settings for terminology service to use within mappings (e.g. lookupDisplay)
   * @param identityServiceSettings    Settings for identity service to use within mappings (e.g. resolveIdentifier)
   * @return
   */
  def executeMappingTask(mappingJobExecution:FhirMappingJobExecution,
                         sourceSettings: Map[String, DataSourceSettings],
                         sinkSettings: FhirSinkSettings,
                         terminologyServiceSettings: Option[TerminologyServiceSettings] = None,
                         identityServiceSettings: Option[IdentityServiceSettings] = None
                        ): Future[Unit]

  /**
   * Execute the given mapping task and return the resulting FhirMappingResult
   *
   * @param mappingJobExecution        Fhir Mapping Job execution
   * @param sourceSettings             Settings for the source system(s)
   * @param terminologyServiceSettings Settings for terminology service to use within mappings (e.g. lookupDisplay)
   * @param identityServiceSettings    Settings for identity service to use within mappings (e.g. resolveIdentifier)
   * @return
   */
  def executeMappingTaskAndReturn(mappingJobExecution:FhirMappingJobExecution,
                                  sourceSettings: Map[String, DataSourceSettings],
                                  terminologyServiceSettings: Option[TerminologyServiceSettings] = None,
                                  identityServiceSettings: Option[IdentityServiceSettings] = None
                                 ): Future[Seq[FhirMappingResult]]
}
