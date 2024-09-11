package io.tofhir.engine.cli.command

import io.tofhir.engine.cli.command
import io.tofhir.engine.model._
import io.tofhir.engine.repository.mapping.IFhirMappingRepository
import io.tofhir.engine.util.FhirMappingJobFormatter
import org.json4s.MappingException

import java.io.FileNotFoundException

class Load extends Command {
  override def execute(args: Seq[String], context: CommandExecutionContext): CommandExecutionContext = {
    if (args.isEmpty) {
      println("load command requires the path of the mapping job definition to load from.")
      context
    } else {
      val filePath = args.head
      if (args.length > 1) {
        println(s"There are more than one arguments to load command. I will only process: $filePath")
      }
      try {
        val mappingJob = FhirMappingJobFormatter.readMappingJobFromFile(filePath)
        println("The following FhirMappingJob successfully loaded.")
        val newContext = command.CommandExecutionContext(context.toFhirEngine, Some(mappingJob), Load.getTaskNameUrlTuples(mappingJob.mappings, context.toFhirEngine.mappingRepo))
        println(Load.serializeMappingJobToCommandLine(newContext))
        newContext
      } catch {
        case _: FileNotFoundException =>
          println(s"The file cannot be found at the specified path:$filePath")
          context
        case _: MappingException =>
          println(s"Invalid MappingJob file at the specified path:$filePath")
          context
      }
    }
  }
}

object Load {
  def getTaskNameUrlTuples(tasks: Seq[FhirMappingTask], mappingRepository: IFhirMappingRepository): Map[String, String] = {
    tasks.foldLeft(Map.empty[String, String]) { (map, task) => // Convert to tuple (name -> url)
      map + (task.name -> task.mappingRef)
    }
  }

  def serializeMappingJobToCommandLine(context: CommandExecutionContext): String = {
    if (context.fhirMappingJob.isEmpty) {
      throw new IllegalStateException("!!! I am trying to serialize the MappingJob from the context, but it is not there!!!")
    }
    val mj = context.fhirMappingJob.get
    val sourceSettingsStr = mj.sourceSettings.map {
      case (sourceAlias, settings: FileSystemSourceSettings) =>
        s"\tFile System Source Settings for '$sourceAlias':\n" +
          s"\t\tName: ${settings.name},\n" +
          s"\t\tSource URI: ${settings.sourceUri},\n" +
          s"\t\tData Folder Path: ${settings.dataFolderPath}\n"
      case (sourceAlias, settings: SqlSourceSettings) =>
        s"\tSql Source Settings for '$sourceAlias':\n" +
          s"\t\tName: ${settings.name},\n" +
          s"\t\tSource URI: ${settings.sourceUri},\n" +
          s"\t\tDatabase URL: ${settings.databaseUrl},\n" +
          s"\t\tUsername: ${settings.username},\n" +
          s"\t\tPassword: ********\n"
      case (sourceAlias, settings: KafkaSourceSettings) =>
        s"\tKafka Source Settings for '$sourceAlias':\n" +
          s"\t\tName: ${settings.name},\n" +
          s"\t\tSource URI: ${settings.sourceUri},\n" +
          s"\t\tBootstrap servers: ${settings.bootstrapServers}\n"
      case _ => "\tNo Source Settings\n"
    }.mkString("\n")
    val sinkSettingsStr = mj.sinkSettings match {
      case settings: FhirRepositorySinkSettings =>
        s"\tFHIR Repository URL: ${settings.fhirRepoUrl}\n"
      case _: FhirSinkSettings => "\tNo Sink Settings\n"
    }

    val tasks = context.mappingNameUrlMap
      .map { case (name, url) => s"\t\t$name -> $url" } // Convert to string
    val tasksStr = "\tTasks:\n" + tasks.mkString("\n")

    s"Mapping Job ID: ${mj.id}\n" + sourceSettingsStr + sinkSettingsStr + tasksStr
  }
}
