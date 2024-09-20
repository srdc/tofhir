package io.tofhir.engine.cli.command

import scala.util.Try

/**
 * Stop one or more running mappings. Its usage is stop <jobID> [<mappingTaskName>].
 */
class Stop extends Command {
  override def execute(args: Seq[String], context: CommandExecutionContext): CommandExecutionContext = {
    if (args.isEmpty || args.size < 2) {
      println("stop command requires job id. Usage stop <jobId> <executionId> [<mappingTaskName>]")
    } else {
      val jobId: String = args.head
      val executionId: String = args(1)
      val mappingTaskName: Option[String] = Try(args(2)).toOption
      mappingTaskName match {
        case None =>
          // Stop all the mappingTasks running inside a job
          context.toFhirEngine.runningJobRegistry.stopJobExecution(jobId, executionId)
        case Some(name) =>
          // Stop a single mappingTask
          context.toFhirEngine.runningJobRegistry.stopMappingExecution(jobId, executionId, name)
      }
      println(s"Processed the command: stop ${args.mkString(" ")}")
    }
    context
  }
}
