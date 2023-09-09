package io.tofhir.engine.cli

import io.tofhir.engine.ToFhirEngine
import io.tofhir.engine.model.{FhirMappingJob, FhirMappingTask}

import scala.concurrent.Future

case class CommandExecutionContext(toFhirEngine: ToFhirEngine,
                                   fhirMappingJob: Option[FhirMappingJob] = None,
                                   mappingNameUrlMap: Map[String, String] = Map.empty) {
}
