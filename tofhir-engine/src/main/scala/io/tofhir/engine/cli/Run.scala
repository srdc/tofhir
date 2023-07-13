package io.tofhir.engine.cli

import io.tofhir.engine.mapping.FhirMappingJobManager
import io.tofhir.engine.model.{FhirMappingJobExecution, FhirMappingTask}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Try

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
            context.toFhirEngine.mappingRepo,
            context.toFhirEngine.contextLoader,
            context.toFhirEngine.schemaLoader,
            context.toFhirEngine.functionLibraries,
            context.toFhirEngine.sparkSession,
            mappingJob.mappingErrorHandling
          )

        val runningStatus =
          if (args.isEmpty) {
            // Execute all tasks in the mapping job
            val f =
              fhirMappingJobManager
                .executeMappingJob(
                  mappingJobExecution = FhirMappingJobExecution(jobId = mappingJob.id, mappingTasks = mappingJob.mappings),
                  sourceSettings = mappingJob.sourceSettings,
                  sinkSettings = mappingJob.sinkSettings,
                  terminologyServiceSettings = mappingJob.terminologyServiceSettings,
                  identityServiceSettings = mappingJob.getIdentityServiceSettings()
                )
            Option.empty[FhirMappingTask] -> f
          } else {
            // Understand whether the argument is the name or the URL of the mapping and then find/execute it.
            if (args.length > 2) {
              println(s"There are more than one arguments to run command. I will only process: ${args.head} and optionaly second")
            }
            val mappingUrl = if (context.mappingNameUrlMap.contains(args.head)) {
              context.mappingNameUrlMap(args.head)
            } else {
              args.head
            }

            val indexAmongMappingToRun = args.drop(1).headOption.flatMap(ind => Try(ind.toInt).toOption).getOrElse(1)

            val task = mappingJob.mappings.filter(_.mappingRef == mappingUrl).drop(indexAmongMappingToRun - 1).headOption
            if (task.isEmpty) {
              println(s"There is no such mapping: $mappingUrl with index $indexAmongMappingToRun")
              task -> Future.failed(new IllegalArgumentException(s"The mapping URL: $mappingUrl cannot be found or invalid mapping task index!"))
            } else {
              val f =
                fhirMappingJobManager
                  .executeMappingTask(
                    mappingJobExecution = FhirMappingJobExecution(mappingTasks = Seq(task.get)),
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
