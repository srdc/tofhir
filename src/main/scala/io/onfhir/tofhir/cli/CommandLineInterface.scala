package io.onfhir.tofhir.cli

import io.onfhir.tofhir.ToFhirEngine
import io.onfhir.tofhir.config.ToFhirConfig
import io.onfhir.tofhir.engine.FhirMappingJobManager
import org.json4s.MappingException

import java.io.FileNotFoundException
import java.util.Scanner
import scala.util.Try

object CommandLineInterface {

  private var commandExecutionContext: CommandExecutionContext = _

  private def init(toFhirEngine: ToFhirEngine): Unit = {
    this.commandExecutionContext =
      if (ToFhirConfig.mappingJobFilePath.isDefined) {
        try {
          val mappingJob = FhirMappingJobManager.readMappingJobFromFile(ToFhirConfig.mappingJobFilePath.get)
          CommandExecutionContext(toFhirEngine = toFhirEngine,
            fhirMappingJob = Some(mappingJob),
            mappingNameUrlMap = Load.getMappingNameUrlTuples(mappingJob.tasks, toFhirEngine.mappingRepository))
        } catch {
          case _: FileNotFoundException =>
            println(s"The file cannot be found at the specified path found in the config:${ToFhirConfig.mappingJobFilePath.get}")
            CommandExecutionContext(toFhirEngine)
          case _: MappingException =>
            println(s"Invalid MappingJob file at the specified path found in the config:${ToFhirConfig.mappingJobFilePath.get}")
            CommandExecutionContext(toFhirEngine)
        }
      } else {
        CommandExecutionContext(toFhirEngine)
      }
  }

  def start(toFhirEngine: ToFhirEngine): Unit = {
    init(toFhirEngine)

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
      val commandName = Try(args.head).getOrElse("")
      val commandArgs = Try(args.tail).getOrElse(Seq.empty[String])
      commandExecutionContext = CommandFactory.apply(commandName).execute(commandArgs, commandExecutionContext)
    }
  }

  private def getWelcomeMessage: String = {
    "Welcome to the CLI of AICCELERATE Data Integration Suite\n" +
      "You can use the help command to see available commands and arguments."
  }

}
