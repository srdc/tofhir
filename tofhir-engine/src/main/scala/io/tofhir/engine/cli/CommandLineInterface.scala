package io.tofhir.engine.cli

import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.ToFhirEngine
import io.tofhir.engine.cli.command.{CommandExecutionContext, CommandFactory, Load}
import io.tofhir.engine.mapping.job.{FhirMappingJobManager, MappingJobScheduler}
import io.tofhir.engine.model.FhirMappingJobExecution
import io.tofhir.engine.util.FhirMappingJobFormatter
import org.json4s.MappingException

import java.io.FileNotFoundException
import java.util.Scanner
import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Try

object CommandLineInterface {

  private var commandExecutionContext: CommandExecutionContext = _

  private def init(toFhirEngine: ToFhirEngine, mappingJobFilePath: Option[String]): Unit = {
    this.commandExecutionContext =
      if (mappingJobFilePath.isDefined) {
        try {
          val mappingJob = FhirMappingJobFormatter.readMappingJobFromFile(mappingJobFilePath.get)
          CommandExecutionContext(toFhirEngine = toFhirEngine,
            fhirMappingJob = Some(mappingJob),
            mappingNameUrlMap = Load.getTaskNameUrlTuples(mappingJob.mappings, toFhirEngine.mappingRepo))
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
    print("\n$ ")
    while (scanner.hasNextLine) {
      val userInput = scanner.nextLine()
      val args = pattern.findAllMatchIn(userInput).map { m =>
        if (m.group(0).startsWith("\"")) m.group(1) // get rid of the quotes (") at the beginning and the end
        else if (m.group(0).startsWith("\'")) m.group(2) // get rid of the quotes (') at the beginning and the end
        else m.group(0)
      }.toSeq
      val commandName = Try(args.head).getOrElse("")
      val commandArgs = Try(args.tail).getOrElse(Seq.empty[String])
      commandExecutionContext = CommandFactory.apply(commandName).execute(commandArgs, commandExecutionContext)
      print("\n$ ")
    }
  }

  private def getWelcomeMessage: String = {
    "Welcome to the CLI of toFHIR Data Integration Engine\n" +
      "You can use the help command to see available commands and arguments."
  }

  /**
   * Run the given mappingJob as a batch and exit the process.
   *
   * @param toFhirEngine
   * @param mappingJobFilePath
   */
  def runJob(toFhirEngine: ToFhirEngine, mappingJobFilePath: Option[String], toFhirDbFolderPath: String): Unit = {
    if (mappingJobFilePath.isEmpty) {
      println("There are no jobs to run. Exiting...")
      System.exit(1)
    }
    val mappingJob = FhirMappingJobFormatter.readMappingJobFromFile(mappingJobFilePath.get)
    if (mappingJob.schedulingSettings.isEmpty) {
      val fhirMappingJobManager =
        new FhirMappingJobManager(
          toFhirEngine.mappingRepo,
          toFhirEngine.contextLoader,
          toFhirEngine.schemaLoader,
          toFhirEngine.functionLibraries,
          toFhirEngine.sparkSession
        )
      val mappingJobExecution = FhirMappingJobExecution(job = mappingJob, mappingTasks = mappingJob.mappings)
      if (mappingJob.sourceSettings.exists(_._2.asStream)) {
        val streamingQueryInitializationTasks: Seq[Future[Unit]] =
          fhirMappingJobManager
            .startMappingJobStream(
              mappingJobExecution,
              sourceSettings = mappingJob.sourceSettings,
              sinkSettings = mappingJob.sinkSettings,
              terminologyServiceSettings = mappingJob.terminologyServiceSettings,
              identityServiceSettings = mappingJob.getIdentityServiceSettings())
            .map(sq => toFhirEngine.runningJobRegistry.registerStreamingQuery(mappingJobExecution, sq._1, sq._2))
            .toSeq
        // Wait for all Futures (i.e. Streaming Queries) to complete
        Await.result(Future.sequence(streamingQueryInitializationTasks), Duration.Inf)

      } else {
        val f =
          fhirMappingJobManager
            .executeMappingJob(
              mappingJobExecution,
              sourceSettings = mappingJob.sourceSettings,
              sinkSettings = mappingJob.sinkSettings,
              terminologyServiceSettings = mappingJob.terminologyServiceSettings,
              identityServiceSettings = mappingJob.getIdentityServiceSettings()
            )
        Await.result(f, Duration.Inf)
      }
    } else {
      val mappingJobScheduler: MappingJobScheduler = MappingJobScheduler.instance(toFhirDbFolderPath)

      val fhirMappingJobManager =
        new FhirMappingJobManager(toFhirEngine.mappingRepo,
          toFhirEngine.contextLoader,
          toFhirEngine.schemaLoader,
          toFhirEngine.functionLibraries,
          toFhirEngine.sparkSession,
          Some(mappingJobScheduler)
        )
      fhirMappingJobManager
        .scheduleMappingJob(
          mappingJobExecution = FhirMappingJobExecution(job = mappingJob, mappingTasks = mappingJob.mappings),
          sourceSettings = mappingJob.sourceSettings,
          sinkSettings = mappingJob.sinkSettings,
          schedulingSettings = mappingJob.schedulingSettings.get,
          terminologyServiceSettings = mappingJob.terminologyServiceSettings,
          identityServiceSettings = mappingJob.getIdentityServiceSettings()
        )
      mappingJobScheduler.scheduler.start()
    }

  }

  /**
   * Parse the command line arguments.
   *
   * @param map  The map where the argumentName -> value pairs are kept. Start with an empty Map[String, Any].
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
      case "--db" :: value :: tail =>
        nextArg(map ++ Map("db" -> value), tail)
      case "--data-dictionary" :: value :: tail =>
        nextArg(map ++ Map("data-dictionary" -> value), tail)
      case "--definition-root-url" :: value :: tail =>
        nextArg(map ++ Map("definition-root-url" -> value), tail)
      case "--encoding" :: value :: tail =>
        nextArg(map ++ Map("encoding" -> value), tail)
      case str :: tail =>
        nextArg(map ++ Map("command" -> str), tail)
      case unknown :: _ =>
        println("Unknown argument " + unknown)
        System.exit(1)
        Map.empty
    }
  }

}
