package io.onfhir.tofhir.cli

import io.onfhir.tofhir.engine.FhirMappingJobManager
import io.onfhir.tofhir.model.{FhirMappingTask, FhirRepositorySinkSettings}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class Run extends Command {
  override def execute(args: Seq[String], context: CommandExecutionContext): CommandExecutionContext = {
    if (context.fhirMappingJob.isEmpty) {
      println("There is no loaded FhirMappingJob! I cannot run anything!")
      context
    } else {
      if (context.runningStatus.isDefined && !context.runningStatus.get._2.isCompleted) {
        println("I am still running some mappings!!! I will not run another one before I finish!!!")
        context
      } else {
        val mappingJob = context.fhirMappingJob.get
        val fhirMappingJobManager =
          new FhirMappingJobManager(
            context.toFhirEngine.mappingRepository,
            context.toFhirEngine.contextLoader,
            context.toFhirEngine.schemaRepository,
            context.toFhirEngine.sparkSession,
            mappingJob.mappingErrorHandling
          )

        val runningStatus =
          if (args.isEmpty) {
            // Execute all tasks in the mapping job
            val f =
              fhirMappingJobManager
                .executeMappingJob(
                  tasks = mappingJob.mappings,
                  sourceSettings = mappingJob.sourceSettings,
                  sinkSettings = mappingJob.sinkSettings,
                  terminologyServiceSettings = mappingJob.terminologyServiceSettings,
                  identityServiceSettings = mappingJob.getIdentityServiceSettings()
                )
            Option.empty[FhirMappingTask] -> f
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
            val task = mappingJob.mappings.find(_.mappingRef == mappingUrl)
            if (task.isEmpty) {
              println(s"There is no such mapping: $mappingUrl")
              task -> Future.failed(new IllegalArgumentException(s"The mapping URL: $mappingUrl cannot be found!"))
            } else {
              val f =
                fhirMappingJobManager
                  .executeMappingTask(
                    task = task.get,
                    sourceSettings = mappingJob.sourceSettings,
                    sinkSettings = mappingJob.sinkSettings,
                    terminologyServiceSettings = mappingJob.terminologyServiceSettings,
                    identityServiceSettings = mappingJob.getIdentityServiceSettings()
                  )
              task -> f
            }
        }

        context.withStatus(Some(runningStatus))
      }
    }
  }
}
