package io.tofhir.engine.cli

import io.tofhir.engine.mapping.FhirMappingJobManager
import io.tofhir.engine.model.{FhirMappingJob, FhirMappingJobExecution}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try

class Run extends Command {
  override def execute(args: Seq[String], context: CommandExecutionContext): CommandExecutionContext = {
    if (context.fhirMappingJob.isEmpty) {
      println("There is no loaded FhirMappingJob! I cannot run anything!")

    } else {
      val mappingJob = context.fhirMappingJob.get

      if (!isStreamingJob(mappingJob)) {
          val fhirMappingJobManager = getFhirMappingJobManager(context, mappingJob)

            if (args.isEmpty) {
              // Execute all tasks in the mapping job
              val mappingJobExecution: FhirMappingJobExecution = FhirMappingJobExecution(job = mappingJob, mappingTasks = mappingJob.mappings)
              val f =
                fhirMappingJobManager
                  .executeMappingJob(
                    mappingJobExecution = mappingJobExecution,
                    sourceSettings = mappingJob.sourceSettings,
                    sinkSettings = mappingJob.sinkSettings,
                    terminologyServiceSettings = mappingJob.terminologyServiceSettings,
                    identityServiceSettings = mappingJob.getIdentityServiceSettings()
                  )
              context.toFhirEngine.runningJobRegistry.registerBatchJob(
                mappingJobExecution,
                f,
                s"Spark job for job: ${mappingJobExecution.job.id} mappings: ${mappingJobExecution.mappingTasks.map(_.mappingRef).mkString(" ")}"
              )
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

              } else {
                val mappingJobExecution: FhirMappingJobExecution = FhirMappingJobExecution(mappingTasks = Seq(task.get), job = mappingJob)
                val f =
                  fhirMappingJobManager
                    .executeMappingTask(
                      mappingJobExecution = mappingJobExecution,
                      sourceSettings = mappingJob.sourceSettings,
                      sinkSettings = mappingJob.sinkSettings,
                      terminologyServiceSettings = mappingJob.terminologyServiceSettings,
                      identityServiceSettings = mappingJob.getIdentityServiceSettings()
                    )
                context.toFhirEngine.runningJobRegistry.registerBatchJob(
                  mappingJobExecution,
                  f,
                  s"Spark job for job: ${mappingJobExecution.job.id} mappings: ${mappingJobExecution.mappingTasks.map(_.mappingRef).mkString(" ")}"
                )
              }
            }

        // Streaming job
      } else {
        val fhirMappingJobManager = getFhirMappingJobManager(context, mappingJob)
        val mappingJobExecution: FhirMappingJobExecution = FhirMappingJobExecution(job = mappingJob, mappingTasks = mappingJob.mappings)
        fhirMappingJobManager.startMappingJobStream(
          mappingJobExecution = mappingJobExecution,
          sourceSettings = mappingJob.sourceSettings,
          sinkSettings = mappingJob.sinkSettings,
          terminologyServiceSettings = mappingJob.terminologyServiceSettings,
          identityServiceSettings = mappingJob.getIdentityServiceSettings()
        )
          .foreach(sq => context.toFhirEngine.runningJobRegistry.registerStreamingQuery(sq._2))
      }
    }
    context
  }

  /**
   * Creates a [[FhirMappingJobManager]] instance from the given context and job
   *
   * @param context
   * @param mappingJob
   * @return
   */
  private def getFhirMappingJobManager(context: CommandExecutionContext, mappingJob: FhirMappingJob): FhirMappingJobManager = {
    new FhirMappingJobManager(
      context.toFhirEngine.mappingRepo,
      context.toFhirEngine.contextLoader,
      context.toFhirEngine.schemaLoader,
      context.toFhirEngine.functionLibraries,
      context.toFhirEngine.sparkSession,
      mappingJob.dataProcessingSettings.mappingErrorHandling,
      context.toFhirEngine.runningJobRegistry
    )
  }

  /**
   * Evaluates whether the given job is a streaming job. For the time being, if at least one of the source settings is configured as a stream,
   * the job is accepted as a streaming job.
   *
   * @param mappingJob
   * @return
   */
  private def isStreamingJob(mappingJob: FhirMappingJob): Boolean = {
    mappingJob.sourceSettings.values.exists(_.asStream)
  }
}
