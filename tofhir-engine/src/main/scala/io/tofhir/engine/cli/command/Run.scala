package io.tofhir.engine.cli.command

import io.tofhir.engine.mapping.job.FhirMappingJobManager
import io.tofhir.engine.model.{FhirMappingJob, FhirMappingJobExecution}

import io.tofhir.engine.Execution.actorSystem.dispatcher

class Run extends Command {
  override def execute(args: Seq[String], context: CommandExecutionContext): CommandExecutionContext = {
    if (context.fhirMappingJob.isEmpty) {
      println("There is no loaded FhirMappingJob! I cannot run anything!")

    } else {
      val fhirMappingJobManager = new FhirMappingJobManager(
        context.toFhirEngine.mappingRepo,
        context.toFhirEngine.contextLoader,
        context.toFhirEngine.schemaLoader,
        context.toFhirEngine.functionLibraries,
        context.toFhirEngine.sparkSession
      )
      val mappingJob = context.fhirMappingJob.get

      if (!isStreamingJob(mappingJob)) {

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
                Some(f),
                s"Spark job for job: ${mappingJobExecution.jobId} mappingTasks: ${mappingJobExecution.mappingTasks.map(_.name).mkString(" ")}"
              )
            } else {
              // Find the mappingTask with given name and execute it
              if (args.length > 1) {
                println(s"There are more than one arguments to run command. I will only process: ${args.head}")
              }

              if(!context.mappingNameUrlMap.contains(args.head)){
                println(s"There are no mappingTask with name ${args.head}!")
              } else {
                val task = mappingJob.mappings.find(_.name == args.head)
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
                  Some(f),
                  s"Spark job for job: ${mappingJobExecution.jobId} mappingTasks: ${mappingJobExecution.mappingTasks.map(_.name).mkString(" ")}"
                )
              }
            }

        // Streaming job
      } else {
        val mappingJobExecution: FhirMappingJobExecution = FhirMappingJobExecution(job = mappingJob, mappingTasks = mappingJob.mappings)
        fhirMappingJobManager.startMappingJobStream(
          mappingJobExecution = mappingJobExecution,
          sourceSettings = mappingJob.sourceSettings,
          sinkSettings = mappingJob.sinkSettings,
          terminologyServiceSettings = mappingJob.terminologyServiceSettings,
          identityServiceSettings = mappingJob.getIdentityServiceSettings()
        )
          .foreach(sq => context.toFhirEngine.runningJobRegistry.registerStreamingQuery(mappingJobExecution, sq._1, sq._2))
      }
    }
    context
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
