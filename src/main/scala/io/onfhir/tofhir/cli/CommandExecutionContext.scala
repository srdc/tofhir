package io.onfhir.tofhir.cli

import io.onfhir.tofhir.ToFhirEngine
import io.onfhir.tofhir.model.{FhirMappingJob, FhirMappingTask}

import scala.concurrent.Future

case class CommandExecutionContext(toFhirEngine: ToFhirEngine,
                                   fhirMappingJob: Option[FhirMappingJob] = None,
                                   mappingNameUrlMap: Map[String, String] = Map.empty,
                                   runningStatus: Option[(Option[FhirMappingTask], Future[Unit])] = None) {

  def withStatus(newRunningStatus: Option[(Option[FhirMappingTask], Future[Unit])]): CommandExecutionContext = {
    this.copy(runningStatus = newRunningStatus)
  }
}
