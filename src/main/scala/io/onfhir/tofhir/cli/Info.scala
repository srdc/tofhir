package io.onfhir.tofhir.cli

import io.onfhir.tofhir.model.{FhirRepositorySinkSettings, FhirSinkSettings}

class Info extends Command {
  override def execute(args: Seq[String], context: CommandExecutionContext): CommandExecutionContext = {
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
    val reposStr = s"Mapping Job ID: ${mj.id}\n" +
      s"\tMapping Repository URI: ${mj.mappingRepositoryUri}\n" +
      s"\tSchema Repository URI: ${mj.schemaRepositoryUri}\n"
    val sinkSettingsStr = mj.sinkSettings match {
      case settings: FhirRepositorySinkSettings =>
        s"\tFHIR Repository URL: ${settings.fhirRepoUrl}\n"
      case _: FhirSinkSettings => "No Sink Settings\n"
    }

    val tasks = context.mappingNameUrlMap
      .map { case (name, url) => s"\t\t$name -> $url" } // Convert to string
    val tasksStr = "\tTasks:\n" + tasks.mkString("\n")

    reposStr + sinkSettingsStr + tasksStr
  }
}
