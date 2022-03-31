package io.onfhir.tofhir.engine

import io.onfhir.tofhir.model.{DataSourceSettings, FhirMappingJob, FhirMappingTask, FhirSinkSettings}
import org.json4s.JObject

import java.util.UUID
import scala.concurrent.Future

/**
 * Interface to submit mapping jobs to tofhir
 */
trait IFhirMappingJobManager {

  /**
   * Execute the given mapping job and write the resulting FHIR resources to given sink
   * @param id                Unique job identifier
   * @param sourceSettings    Data source settings and configurations
   * @param tasks             Mapping tasks that will be executed in sequential
   * @param sinkSettings      FHIR sink settings (can be a FHIR repository, file system, kafka)
   * @return
   */
  def executeMappingJob(id:String = UUID.randomUUID().toString,
                        sourceSettings:DataSourceSettings,
                        tasks:Seq[FhirMappingTask],
                        sinkSettings:FhirSinkSettings):Future[Unit]

  /**
   * Execute the given mapping job and return the resulting FHIR resources
   * @param id                Unique job identifier
   * @param sourceSettings    Data source settings and configurations
   * @param tasks             Mapping tasks that will be executed in sequential
   * @return
   */
  def executeMappingJobAndReturn(id:String = UUID.randomUUID().toString,
                                 sourceSettings:DataSourceSettings,
                                 tasks:Seq[FhirMappingTask]):Future[Seq[JObject]]
}