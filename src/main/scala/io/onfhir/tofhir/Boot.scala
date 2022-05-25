package io.onfhir.tofhir

import io.onfhir.tofhir.cli.CommandLineInterface
import io.onfhir.tofhir.config.{MappingErrorHandling, ToFhirConfig}

/**
 * Entrypoint of toFHIR
 */
object Boot extends App {
  val toFhirEngine = new ToFhirEngine(ToFhirConfig.appName, ToFhirConfig.sparkMaster,
    ToFhirConfig.mappingRepositoryFolderPath, ToFhirConfig.schemaRepositoryFolderPath)

  CommandLineInterface.start(toFhirEngine)
}
