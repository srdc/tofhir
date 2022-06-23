package io.onfhir.tofhir.cli

import io.onfhir.tofhir.ToFhirEngine
import io.onfhir.tofhir.engine.FhirMappingJobManager
import org.json4s.MappingException

import java.io.FileNotFoundException
import java.util.Scanner
import scala.annotation.tailrec
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.Try

object CommandLineInterface {

  private var commandExecutionContext: CommandExecutionContext = _

  private def init(toFhirEngine: ToFhirEngine, mappingJobFilePath: Option[String]): Unit = {
    this.commandExecutionContext =
      if (mappingJobFilePath.isDefined) {
        try {
          val mappingJob = FhirMappingJobManager.readMappingJobFromFile(mappingJobFilePath.get)
          CommandExecutionContext(toFhirEngine = toFhirEngine,
            fhirMappingJob = Some(mappingJob),
            mappingNameUrlMap = Load.getMappingNameUrlTuples(mappingJob.tasks, toFhirEngine.mappingRepository))
        } catch {
          case _: FileNotFoundException =>
            println(s"The file cannot be found at the specified path found in the config:${mappingJobFilePath.get}")
            CommandExecutionContext(toFhirEngine)
          case _: MappingException =>
            println(s"Invalid MappingJob file at the specified path found in the config:${mappingJobFilePath.get}")
            CommandExecutionContext(toFhirEngine)
        }
      } else {
        CommandExecutionContext(toFhirEngine)
      }
  }

  /**
   * Start the interactive CLI so that the user can issue commands through the standard input.
   *
   * @param toFhirEngine
   * @param mappingJobFilePath
   */
  def start(toFhirEngine: ToFhirEngine, mappingJobFilePath: Option[String] = None): Unit = {
    init(toFhirEngine, mappingJobFilePath)

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

  /**
   * Run the given mappingJob as a batch and exit the process.
   *
   * @param toFhirEngine
   * @param mappingJobFilePath
   */
  def runJob(toFhirEngine: ToFhirEngine, mappingJobFilePath: Option[String]): Unit = {
    if(mappingJobFilePath.isEmpty) {
      println("There are no jobs to run. Exiting...")
      System.exit(1)
    }
    val mappingJob = FhirMappingJobManager.readMappingJobFromFile(mappingJobFilePath.get)
    val fhirMappingJobManager = new FhirMappingJobManager(toFhirEngine.mappingRepository, toFhirEngine.contextLoader,
      toFhirEngine.schemaRepository, toFhirEngine.sparkSession, mappingJob.mappingErrorHandling)
    if(mappingJob.cronExpression.isEmpty) {
      val f = fhirMappingJobManager.executeMappingJob(tasks = mappingJob.tasks, sinkSettings = mappingJob.sinkSettings)
      Await.result(f, Duration.Inf)
    } else {
      fhirMappingJobManager.scheduleMappingJob(tasks = mappingJob.tasks, sinkSettings = mappingJob.sinkSettings, cronExpression = mappingJob.cronExpression.get)
    }

  }

  /**
   * Parse the command line arguments.
   *
   * @param map The map where the argumentName -> value pairs are kept. Start with an empty Map[String, Any].
   * @param list The list of the arguments
   * @return
   */
  @tailrec
  def nextArg(map: Map[String, Any], list: List[String]): Map[String, Any] = {
    list match {
      case Nil => map
      case "--job" :: value :: tail =>
        nextArg(map ++ Map("job" -> value), tail)
      case "--mappings" :: value :: tail =>
        nextArg(map ++ Map("mappings" -> value), tail)
      case "--schemas" :: value :: tail =>
        nextArg(map ++ Map("schemas" -> value), tail)
      case str :: tail =>
        nextArg(map ++ Map("command" -> str), tail)
      case unknown :: _ =>
        println("Unknown argument " + unknown)
        System.exit(1)
        Map.empty
    }
  }

}