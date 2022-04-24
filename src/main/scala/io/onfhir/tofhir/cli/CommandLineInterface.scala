package io.onfhir.tofhir.cli

import io.onfhir.tofhir.config.ToFhirConfig
import io.onfhir.tofhir.engine.FhirMappingJobManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.json4s.MappingException

import java.io.FileNotFoundException
import java.util.Scanner

object CommandLineInterface {

  private var commandExecutionContext: CommandExecutionContext = _

  private def init(): Unit = {

    val sparkConf: SparkConf = new SparkConf()
      .setAppName(ToFhirConfig.appName)
      .setMaster(ToFhirConfig.sparkMaster)
      .set("spark.driver.allowMultipleContexts", "false")
      .set("spark.ui.enabled", "false")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    this.commandExecutionContext =
      if (ToFhirConfig.mappingJobsFilePath.isDefined) {
        try {
          val mappingJobs = FhirMappingJobManager.readMappingJobFromFile(ToFhirConfig.mappingJobsFilePath.get)
          new CommandExecutionContext(sparkSession = sparkSession,
            fhirMappingJob = Some(mappingJobs.head),
            mappingNameUrlMap = Load.getMappingNameUrlTuples(mappingJobs.head)) // We can only process a single MappingJob for now
        } catch {
          case _: FileNotFoundException =>
            println(s"The file cannot be found at the specified path found in the config:${ToFhirConfig.mappingJobsFilePath.get}")
            new CommandExecutionContext(sparkSession)
          case _: MappingException =>
            println(s"Invalid MappingJob file at the specified path found in the config:${ToFhirConfig.mappingJobsFilePath.get}")
            new CommandExecutionContext(sparkSession)
        }
      } else {
        new CommandExecutionContext(sparkSession)
      }
  }

  def start(): Unit = {
    init()

    print(getWelcomeMessage)
    println()

    val pattern = """[^\s"']+|"([^"]*)"|'([^']*)'""".r // Regex to parse the command and the arguments
    val scanner = new Scanner(System.in)
    while (true) {
      print("\n$ ")
      val userInput = scanner.nextLine()
      val args = pattern.findAllMatchIn(userInput).map { m =>
        if (m.group(0).startsWith("\"")) m.group(1) // get rid of the quotes (") at the beginning and the end
        else if (m.group(0).startsWith("\'")) m.group(2) // get rid of the quotes (') at the beginning and the end
        else m.group(0)
      }.toSeq
      val commandName = args.head
      val commandArgs = args.tail
      commandExecutionContext = CommandFactory.apply(commandName).execute(commandArgs, commandExecutionContext)
    }
  }

  private def getWelcomeMessage: String = {
    "Welcome to the CLI of AICCELERATE Data Integration Suite\n" +
      "You can use the help command to see available commands and arguments."
  }

}
