package io.onfhir.tofhir.cli

import io.onfhir.tofhir.engine.{FhirMappingFolderRepository, FhirMappingJobManager}
import io.onfhir.tofhir.model.FhirMappingJob
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
        val mappingJobs = FhirMappingJobManager.readMappingJobFromFile(filePath)
        println("The following FhirMappingJob successfully loaded.")
        val newContext = CommandExecutionContext(context.sparkSession, Some(mappingJobs.head), Load.getMappingNameUrlTuples(mappingJobs.head))
        println(Info.serializeMappingJobToCommandLine(newContext))
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
  def getMappingNameUrlTuples(mappingJob: FhirMappingJob): Map[String, String] = {
    val repo = new FhirMappingFolderRepository(mappingJob.mappingRepositoryUri)
    mappingJob.tasks.foldLeft(Map.empty[String, String]) { (map, task) => // Convert to tuple (name -> url)
      map + (repo.getFhirMappingByUrl(task.mappingRef).name -> task.mappingRef)
    }
  }
}
