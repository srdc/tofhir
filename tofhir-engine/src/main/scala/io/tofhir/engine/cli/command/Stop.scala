package io.tofhir.engine.cli.command

import scala.util.Try

/**
 * Stop one or more running mappings. Its usage is stop <jobID> [<mappingUrl>].
 */
class Stop extends Command {
  override def execute(args: Seq[String], context: CommandExecutionContext): CommandExecutionContext = {
    if (args.isEmpty || args.size < 2) {
      println("stop command requires job id. Usage stop <jobId> <executionId> [<mappingUrl>]")
    } else {
      val jobId: String = args.head
      val executionId: String = args(1)
      val mappingUrl: Option[String] = Try(args(2)).toOption
      mappingUrl match {
        case None =>
          // Stop all the mappings running inside a job
          context.toFhirEngine.runningJobRegistry.stopJobExecution(jobId, executionId)
        case Some(url) =>
          // Stop a single mapping
          context.toFhirEngine.runningJobRegistry.stopMappingExecution(jobId, executionId, url)
      }
      println(s"Processed the command: stop ${args.mkString(" ")}")
    }
    context
  }
}
