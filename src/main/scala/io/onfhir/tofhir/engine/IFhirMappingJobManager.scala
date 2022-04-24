package io.onfhir.tofhir.engine

import io.onfhir.tofhir.model.{DataSourceSettings, FhirMappingSourceContext, FhirMappingTask, FhirSinkSettings}
import org.json4s.JObject

import java.util.UUID
import scala.concurrent.Future

/**
 * Interface to submit mapping jobs to tofhir
 */
trait IFhirMappingJobManager {

  /**
   * Execute the given mapping job and write the resulting FHIR resources to the given sink
   *
   * @param id           Unique job identifier
   * @param tasks        Mapping tasks that will be executed in sequential
   * @param sinkSettings FHIR sink settings (can be a FHIR repository, file system, kafka)
   * @return
   */
  def executeMappingJob(id: String = UUID.randomUUID().toString,
                        tasks: Seq[FhirMappingTask],
                        sinkSettings: FhirSinkSettings): Future[Unit]

  /**
   * Execute the given mapping task and write the resulting FHIR resources to the given sink
   *
   * @param id           Unique job identifier
   * @param task         A Mapping task that will be executed
   * @param sinkSettings FHIR sink settings (can be a FHIR repository, file system, kafka)
   * @return
   */
  def executeMappingTask(id: String = UUID.randomUUID().toString,
                         task: FhirMappingTask,
                         sinkSettings: FhirSinkSettings): Future[Unit]

  /**
   * Execute the given mapping task and return the resulting FHIR resources
   *
   * @param id   Unique job identifier
   * @param task Mapping task that will be executed
   * @return
   */
  def executeMappingTaskAndReturn(id: String = UUID.randomUUID().toString,
                                  task: FhirMappingTask): Future[Seq[JObject]]
}
