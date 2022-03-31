package io.onfhir.tofhir.engine

import io.onfhir.tofhir.model.{DataSourceSettings, FhirMappingTask, FhirSinkSettings}
import org.json4s.JObject

import scala.concurrent.Future

class FhirMappingJobManager(fhirMappingRepository:IFhirMappingRepository) extends IFhirMappingJobManager {
  /**
   * Execute the given mapping job and write the resulting FHIR resources to given sink
   *
   * @param id             Unique job identifier
   * @param sourceSettings Data source settings and configurations
   * @param tasks          Mapping tasks that will be executed in sequential
   * @param sinkSettings   FHIR sink settings (can be a FHIR repository, file system, kafka)
   * @return
   */
  override def executeMappingJob(id: String, sourceSettings: DataSourceSettings, tasks: Seq[FhirMappingTask], sinkSettings: FhirSinkSettings): Future[Unit] = ???

  /**
   * Execute the given mapping job and return the resulting FHIR resources
   *
   * @param id             Unique job identifier
   * @param sourceSettings Data source settings and configurations
   * @param tasks          Mapping tasks that will be executed in sequential
   * @return
   */
  override def executeMappingJobAndReturn(id:  String, sourceSettings:  DataSourceSettings, tasks: Seq[FhirMappingTask]): Future[Seq[JObject]] = ???
}
