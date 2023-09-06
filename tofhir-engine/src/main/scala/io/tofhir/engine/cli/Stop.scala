package io.tofhir.engine.cli

/**
 * Stop one or more running mappings. Its usage is stop <jobID> [<mappingUrl>].
 */
class Stop extends Command {
  override def execute(args: Seq[String], context: CommandExecutionContext): CommandExecutionContext = {
    if (args.isEmpty) {
      println("stop command requires job id. Usage stop <jobId> [<mappingUrl>]")
    } else {
      val jobId: String = args.head
      val mappingUrl: Option[String] = args.tail.headOption
      mappingUrl match {
        case None =>
          // Stop all the mappings running inside a job
          context.toFhirEngine.runningJobRegistry.stopJobExecution(jobId)
        case Some(url) =>
          // Stop a single mapping
          context.toFhirEngine.runningJobRegistry.stopMappingExecution(jobId, url)
      }
      println(s"Processed the command: stop ${args.mkString(" ")}")
    }
    context
  }
}