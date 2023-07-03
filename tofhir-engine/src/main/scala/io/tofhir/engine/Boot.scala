package io.tofhir.engine

import io.onfhir.path.IFhirPathFunctionLibraryFactory
import io.tofhir.engine.cli.{CommandExecutionContext, CommandFactory, CommandLineInterface}
import io.tofhir.engine.config.ToFhirConfig

/**
 * Entrypoint of toFHIR
 */
object Boot extends App {
  init(args)

  def init(args: Array[String], functionLibraryFactories : Map[String, IFhirPathFunctionLibraryFactory] = Map.empty): Unit = {
    val options = CommandLineInterface.nextArg(Map(), args.toList)
    //Interactive command line interface
    if (options.isEmpty || !options.contains("command") || options("command").asInstanceOf[String] == "cli") {
      val toFhirEngine = new ToFhirEngine(functionLibraryFactories = functionLibraryFactories)
      CommandLineInterface.start(toFhirEngine, ToFhirConfig.engineConfig.initialMappingJobFilePath)
    }
    // Extract schemas from a REDCap data dictionary
    else if (options("command").asInstanceOf[String] == "extract-redcap-schemas") {
      val toFhirEngine = new ToFhirEngine(functionLibraryFactories = functionLibraryFactories)
      // get parameters
      val dataDictionary = options.get("data-dictionary").map(_.asInstanceOf[String])
      val definitionRootUrl = options.get("definition-root-url").map(_.asInstanceOf[String])
      val encoding = options.get("encoding").map(_.asInstanceOf[String])
      val commandArgs: Seq[String] = Seq(dataDictionary, definitionRootUrl, encoding).filter(arg => arg.nonEmpty).map(arg => arg.get)
      // run command
      CommandFactory.apply("extract-redcap-schemas").execute(commandArgs, CommandExecutionContext(toFhirEngine))
    }
    //Run as batch job
    else if (options("command").asInstanceOf[String] == "run") {
      val toFhirEngine = new ToFhirEngine(functionLibraryFactories = functionLibraryFactories)
      val mappingJobFilePath =
        if (options.contains("job"))
          options.get("job").map(_.asInstanceOf[String])
        else
          ToFhirConfig.engineConfig.initialMappingJobFilePath

      val toFhirDbFolderPath =
        if (options.contains("db-path")) options("db-path").asInstanceOf[String]
        else ToFhirConfig.engineConfig.toFhirDbFolderPath

      CommandLineInterface.runJob(toFhirEngine, mappingJobFilePath, toFhirDbFolderPath)
    }
  }
}
