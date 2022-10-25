package io.tofhir.engine.cli

import io.tofhir.engine.model.{DataSourceSettings, FhirRepositorySinkSettings, FhirSinkSettings, FileSystemSourceSettings, KafkaSourceSettings, SqlSourceSettings}

import scala.util.{Failure, Success}

class Info extends Command {
  override def execute(args: Seq[String], context: CommandExecutionContext): CommandExecutionContext = {
    if(context.runningStatus.isDefined) {
      if(context.runningStatus.get._2.isCompleted) {
        context.runningStatus.get._2.value match {
          case Some(Success(_)) =>
            if(context.runningStatus.get._1.isDefined) {
              println(s"The execution of the single mapping task was successful. URL: ${context.runningStatus.get._1.get.mappingRef}")
            } else {
              println("The execution of the tasks in the Mapping Job was successful.")
            }
          case Some(Failure(ex)) =>
            if(context.runningStatus.get._1.isDefined) {
              println(s"Error in the execution of the single mapping task with URL: ${context.runningStatus.get._1.get.mappingRef}")
            } else {
              println("Error in the execution of the tasks in the Mapping Job.")
            }
            println("ERROR MESSAGE OF THE PREVIOUS EXECUTION:")
            println(ex.getMessage + "\n")
          case None =>
            // This should not be the case since I already do an isCompleted check, but let's remove the compiler warning
            println("I am still running the task(s) defined in the loaded Mapping Job.")
        }
      } else {
        if(context.runningStatus.get._1.isDefined) {
          println(s"I am still running the single mapping task with URL: ${context.runningStatus.get._1.get.mappingRef}")
        } else {
          println("I am still running the tasks defined in the loaded Mapping Job.")
        }
      }
    }
    if (context.fhirMappingJob.isEmpty) {
      println("There is no loaded FhirMappingJob!")
    } else {
      println("The following FhirMappingJob is available")
      println(Info.serializeMappingJobToCommandLine(context))
    }
    context
  }
}

object Info {
  def serializeMappingJobToCommandLine(context: CommandExecutionContext): String = {
    if(context.fhirMappingJob.isEmpty) {
      throw new IllegalStateException("!!! I am trying to serialize the MappingJob from the context, but it is not there!!!")
    }
    val mj = context.fhirMappingJob.get
    val sourceSettingsStr = mj.sourceSettings.map {
      case (sourceAlias, settings: FileSystemSourceSettings) =>
        s"\tFile System Source Settings for '$sourceAlias':\n" +
          s"\t\tName: ${settings.name},\n" +
          s"\t\tSource URI: ${settings.sourceUri},\n" +
          s"\t\tData Folder Path: ${settings.dataFolderPath}\n"
      case  (sourceAlias,settings: SqlSourceSettings) =>
        s"\tSql Source Settings for '$sourceAlias':\n" +
          s"\t\tName: ${settings.name},\n" +
          s"\t\tSource URI: ${settings.sourceUri},\n" +
          s"\t\tDatabase URL: ${settings.databaseUrl},\n" +
          s"\t\tUsername: ${settings.username},\n" +
          s"\t\tPassword: ********\n"
      case (sourceAlias, settings:KafkaSourceSettings) =>
        s"\tKafka Source Settings for '$sourceAlias':\n" +
          s"\t\tName: ${settings.name},\n" +
          s"\t\tSource URI: ${settings.sourceUri},\n" +
          s"\t\tBootstrap servers: ${settings.bootstrapServers}\n"
      case  _ => "\tNo Source Settings\n"
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
