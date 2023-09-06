package io.tofhir.engine.cli

/**
 * Command to list the running mapping tasks.
 */
class ListRunningMappings extends Command {
  override def execute(args: Seq[String], context: CommandExecutionContext): CommandExecutionContext = {
    context.toFhirEngine.runningJobRegistry.getRunningExecutions() match {
      case m:Map[String, Seq[String]] if m.isEmpty => println("There is no running task at the moment")
      case runningMappings => runningMappings.foreach(jobAndMappings => {
        println(s"Job: ${jobAndMappings._1}")
        jobAndMappings._2.foreach(mapping => println(s"\t$mapping"))
      })
    }
    context
  }
}
