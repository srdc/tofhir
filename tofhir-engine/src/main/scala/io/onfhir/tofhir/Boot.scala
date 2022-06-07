package io.onfhir.tofhir

import io.onfhir.tofhir.cli.CommandLineInterface
import io.onfhir.tofhir.config.ToFhirConfig

/**
 * Entrypoint of toFHIR
 */
object Boot extends App {

  val options = CommandLineInterface.nextArg(Map(), args.toList)

  if (options.isEmpty || !options.contains("command") || options("command").asInstanceOf[String] == "cli") {
    val toFhirEngine = new ToFhirEngine(ToFhirConfig.appName, ToFhirConfig.sparkMaster,
      ToFhirConfig.mappingRepositoryFolderPath, ToFhirConfig.schemaRepositoryFolderPath)
    CommandLineInterface.start(toFhirEngine, ToFhirConfig.mappingJobFilePath)
  } else if (options("command").asInstanceOf[String] == "run") {
    val toFhirEngine = new ToFhirEngine(ToFhirConfig.appName, ToFhirConfig.sparkMaster,
      options.get("mappings").map(_.asInstanceOf[String]).getOrElse(ToFhirConfig.mappingRepositoryFolderPath),
      options.get("schemas").map(_.asInstanceOf[String]).getOrElse(ToFhirConfig.schemaRepositoryFolderPath))
    val mappingJobFilePath =
      if (options.contains("job"))
        options.get("job").map(_.asInstanceOf[String])
      else ToFhirConfig.mappingJobFilePath
    CommandLineInterface.runJob(toFhirEngine, mappingJobFilePath)
  }

}
