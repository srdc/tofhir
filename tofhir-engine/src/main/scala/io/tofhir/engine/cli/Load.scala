package io.tofhir.engine.cli

import io.tofhir.engine.cli
import io.tofhir.engine.mapping.IFhirMappingRepository
import io.tofhir.engine.model.FhirMappingTask
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
        val newContext = cli.CommandExecutionContext(context.toFhirEngine, Some(mappingJob), Load.getMappingNameUrlTuples(mappingJob.mappings, context.toFhirEngine.mappingRepository))
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
  def getMappingNameUrlTuples(tasks: Seq[FhirMappingTask], mappingRepository: IFhirMappingRepository): Map[String, String] = {
    tasks.foldLeft(Map.empty[String, String]) { (map, task) => // Convert to tuple (name -> url)
      map + (mappingRepository.getFhirMappingByUrl(task.mappingRef).name -> task.mappingRef)
    }
  }
}
