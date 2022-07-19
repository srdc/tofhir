package io.onfhir.tofhir.engine

import io.onfhir.tofhir.model.{DataSourceSettings, FhirMappingTask, FhirSinkSettings, SchedulingSettings}
import org.apache.spark.sql.streaming.StreamingQuery
import org.json4s.JObject

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
   * @param id              Unique job identifier
   * @param tasks           Mapping tasks that will be executed in sequential
   * @param sourceSettings  Settings for the source system(s)
   * @param sinkSettings    FHIR sink settings (can be a FHIR repository, file system, kafka)
   * @param timeRange       If given, execute the mapping job for data between the given interval
   * @return
   */
  def executeMappingJob(id: String = UUID.randomUUID().toString,
                        tasks: Seq[FhirMappingTask],
                        sourceSettings: Map[String,DataSourceSettings],
                        sinkSettings: FhirSinkSettings,
                        timeRange: Option[(LocalDateTime, LocalDateTime)] = Option.empty): Future[Unit]

  /**
   * Start streaming mapping job
   *
   * @param id              Job identifier
   * @param tasks           Mapping tasks that will be executed in parallel in stream mode
   * @param sourceSettings  Settings for the source system(s)
   * @param sinkSettings    FHIR sink settings (can be a FHIR repository, file system, kafka)
   * @return
   */
  def startMappingJobStream(id: String = UUID.randomUUID().toString,
                            tasks: Seq[FhirMappingTask],
                            sourceSettings: Map[String,DataSourceSettings],
                            sinkSettings: FhirSinkSettings): StreamingQuery

  /**
   * Schedule to execute the given mapping job with given cron expression and write the resulting FHIR resources to the given sink
   * @param id                    Job identifier
   * @param tasks                 Mapping tasks that will be scheduled
   * @param sourceSettings        Settings for the source system(s)
   * @param sinkSettings          FHIR sink settings (can be a FHIR repository, file system, kafka)
   * @param schedulingSettings    Scheduling info
   * @return
   */

  def scheduleMappingJob(id: String = UUID.randomUUID().toString,
                        tasks: Seq[FhirMappingTask],
                        sourceSettings: Map[String,DataSourceSettings],
                        sinkSettings: FhirSinkSettings,
                        schedulingSettings: SchedulingSettings): Unit
  /**
   * Execute the given mapping task and write the resulting FHIR resources to the given sink
   *
   * @param id                Unique job identifier
   * @param task              A Mapping task that will be executed
   * @param sourceSettings    Settings for the source system(s)
   * @param sinkSettings      FHIR sink settings (can be a FHIR repository, file system, kafka)
   * @return
   */
  def executeMappingTask(id: String = UUID.randomUUID().toString,
                         task: FhirMappingTask,
                         sourceSettings: Map[String,DataSourceSettings],
                         sinkSettings: FhirSinkSettings): Future[Unit]

  /**
   * Execute the given mapping task and return the resulting FHIR resources
   *
   * @param id   Unique job identifier
   * @param task Mapping task that will be executed
   * @param sourceSettings    Settings for the source system(s)
   * @return
   */
  def executeMappingTaskAndReturn(id: String = UUID.randomUUID().toString,
                                  task: FhirMappingTask,
                                  sourceSettings: Map[String,DataSourceSettings],
                                 ): Future[Seq[JObject]]
}
