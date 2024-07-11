package io.tofhir.engine.cli.command

import io.tofhir.engine.ToFhirEngine
import io.tofhir.engine.model.FhirMappingJob

case class CommandExecutionContext(toFhirEngine: ToFhirEngine,
                                   fhirMappingJob: Option[FhirMappingJob] = None,
                                   mappingNameUrlMap: Map[String, String] = Map.empty) {
}
