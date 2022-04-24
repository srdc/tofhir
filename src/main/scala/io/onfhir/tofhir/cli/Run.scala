package io.onfhir.tofhir.cli

import io.onfhir.tofhir.engine.{FhirMappingFolderRepository, FhirMappingJobManager, MappingContextLoader, SchemaFolderRepository}

import scala.concurrent.ExecutionContext.Implicits.global

class Run extends Command {
  override def execute(args: Seq[String], context: CommandExecutionContext): CommandExecutionContext = {
    if (context.fhirMappingJob.isEmpty) {
      println("There is no loaded FhirMappingJob! I cannot run anything!")
    } else {
      val mappingJob = context.fhirMappingJob.get
      val fhirMappingRepository = new FhirMappingFolderRepository(mappingJob.mappingRepositoryUri)
      val contextLoader = new MappingContextLoader(fhirMappingRepository)
      val schemaRepository = new SchemaFolderRepository(mappingJob.schemaRepositoryUri)
      val fhirMappingJobManager = new FhirMappingJobManager(fhirMappingRepository, contextLoader, schemaRepository, context.sparkSession)

      if (args.isEmpty) {
        // Execute all tasks in the mapping job
        fhirMappingJobManager.executeMappingJob(tasks = mappingJob.tasks, sinkSettings = mappingJob.sinkSettings)
      } else {
        // Understand whether the argument is the name or the URL of the mapping and then find/execute it.
        if (args.length > 1) {
          println(s"There are more than one arguments to run command. I will only process: ${args.head}")
        }
        val mappingUrl = if (context.mappingNameUrlMap.contains(args.head)) {
          context.mappingNameUrlMap(args.head)
        } else {
          args.head
        }
        val task = mappingJob.tasks.find(_.mappingRef == mappingUrl)
        if (task.isEmpty) {
          println(s"There is no such mapping: $mappingUrl")
        } else {
          fhirMappingJobManager.executeMappingTask(task = task.get, sinkSettings = mappingJob.sinkSettings)
        }
      }
    }
    context
  }
}
