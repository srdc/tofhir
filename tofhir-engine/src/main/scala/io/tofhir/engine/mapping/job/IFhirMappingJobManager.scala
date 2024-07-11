package io.tofhir.engine.mapping.job

import io.tofhir.engine.model._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.StreamingQuery

import java.time.LocalDateTime
import scala.concurrent.Future

/**
 * Interface to submit mapping jobs to tofhir
 */
trait IFhirMappingJobManager {

  /**
   * Execute the given mapping job and write the resulting FHIR resources to the given sink
   *
   * @param mappingJobExecution        Fhir Mapping Job execution
   * @param sourceSettings             Source settings of the mapping job
   * @param sinkSettings               FHIR sink settings (can be a FHIR repository, file system, kafka)
   * @param terminologyServiceSettings Settings for terminology service to use within mappings (e.g. lookupDisplay)
   * @param identityServiceSettings    Settings for identity service to use within mappings (e.g. resolveIdentifier)
   * @param timeRange                  If given, execute the mapping job for data between the given interval
   * @return
   */
  def executeMappingJob(mappingJobExecution: FhirMappingJobExecution,
                        sourceSettings: Map[String, MappingJobSourceSettings],
                        sinkSettings: FhirSinkSettings,
                        terminologyServiceSettings: Option[TerminologyServiceSettings] = None,
                        identityServiceSettings: Option[IdentityServiceSettings] = None,
                        timeRange: Option[(LocalDateTime, LocalDateTime)] = None): Future[Unit]

  /**
   * Start streaming mapping job. A [[Future]] of [[StreamingQuery]] is returned for each mapping task included in the job, encapsulated in a map.
   *
   * @param mappingJobExecution        Fhir Mapping Job execution
   * @param sourceSettings             Source settings of the mapping job
   * @param sinkSettings               FHIR sink settings (can be a FHIR repository, file system, kafka)
   * @param terminologyServiceSettings Settings for terminology service to use within mappings (e.g. lookupDisplay)
   * @param identityServiceSettings    Settings for identity service to use within mappings (e.g. resolveIdentifier)
   * @return A map of (mapping url -> streaming query futures).
   */
  def startMappingJobStream(mappingJobExecution: FhirMappingJobExecution,
                            sourceSettings: Map[String, MappingJobSourceSettings],
                            sinkSettings: FhirSinkSettings,
                            terminologyServiceSettings: Option[TerminologyServiceSettings] = None,
                            identityServiceSettings: Option[IdentityServiceSettings] = None,
                           ): Map[String, Future[StreamingQuery]]

  /**
   * Schedule to execute the given mapping job with given cron expression and write the resulting FHIR resources to the given sink
   *
   * @param mappingJobExecution        Fhir Mapping Job execution
   * @param sourceSettings             Source settings of the mapping job
   * @param sinkSettings               FHIR sink settings (can be a FHIR repository, file system, kafka)
   * @param schedulingSettings         Scheduling info
   * @param terminologyServiceSettings Settings for terminology service to use within mappings (e.g. lookupDisplay)
   * @param identityServiceSettings    Settings for identity service to use within mappings (e.g. resolveIdentifier)
   * @return
   */

  def scheduleMappingJob(mappingJobExecution: FhirMappingJobExecution,
                         sourceSettings: Map[String, MappingJobSourceSettings],
                         sinkSettings: FhirSinkSettings,
                         schedulingSettings: BaseSchedulingSettings,
                         terminologyServiceSettings: Option[TerminologyServiceSettings] = None,
                         identityServiceSettings: Option[IdentityServiceSettings] = None): Unit

  /**
   * Execute the given mapping task and write the resulting FHIR resources to the given sink
   *
   * @param mappingJobExecution        Fhir Mapping Job execution
   * @param sourceSettings             Source settings of the mapping job
   * @param sinkSettings               FHIR sink settings (can be a FHIR repository, file system, kafka)
   * @param terminologyServiceSettings Settings for terminology service to use within mappings (e.g. lookupDisplay)
   * @param identityServiceSettings    Settings for identity service to use within mappings (e.g. resolveIdentifier)
   * @return
   */
  def executeMappingTask(mappingJobExecution: FhirMappingJobExecution,
                         sourceSettings: Map[String, MappingJobSourceSettings],
                         sinkSettings: FhirSinkSettings,
                         terminologyServiceSettings: Option[TerminologyServiceSettings] = None,
                         identityServiceSettings: Option[IdentityServiceSettings] = None
                        ): Future[Unit]

  /**
   * Execute the given mapping task and return the resulting FhirMappingResult
   *
   * @param mappingJobExecution        Fhir Mapping Job execution
   * @param sourceSettings             Source settings of the mapping job
   * @param terminologyServiceSettings Settings for terminology service to use within mappings (e.g. lookupDisplay)
   * @param identityServiceSettings    Settings for identity service to use within mappings (e.g. resolveIdentifier)
   * @return
   */
  def executeMappingTaskAndReturn(mappingJobExecution: FhirMappingJobExecution,
                                  sourceSettings: Map[String, MappingJobSourceSettings],
                                  terminologyServiceSettings: Option[TerminologyServiceSettings] = None,
                                  identityServiceSettings: Option[IdentityServiceSettings] = None
                                 ): Future[Seq[FhirMappingResult]]

  /**
   * Executes the specified FHIR mapping job and returns the resulting FhirMappingResult dataset.
   *
   * @param mappingJobExecution        The FHIR Mapping Job execution details.
   * @param sourceSettings             Source settings of the mapping job
   * @param terminologyServiceSettings Optional settings for the terminology service to use within mappings (e.g., for lookupDisplay).
   * @param identityServiceSettings    Optional settings for the identity service to use within mappings (e.g., for resolveIdentifier).
   * @param taskCompletionCallback     A callback function to be invoked when a mapping task execution is completed.
   * @return A Future containing a Dataset of FhirMappingResult representing the outcome of the mapping job.
   */
  def executeMappingJobAndReturn(mappingJobExecution: FhirMappingJobExecution,
                                 sourceSettings: Map[String, MappingJobSourceSettings],
                                 terminologyServiceSettings: Option[TerminologyServiceSettings] = None,
                                 identityServiceSettings: Option[IdentityServiceSettings] = None,
                                 taskCompletionCallback: () => Unit
                                ): Future[Dataset[FhirMappingResult]]

}
