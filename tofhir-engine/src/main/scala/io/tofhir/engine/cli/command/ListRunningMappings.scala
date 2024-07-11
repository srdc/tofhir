package io.tofhir.engine.cli.command

/**
 * Command to list the running mapping tasks.
 */
class ListRunningMappings extends Command {
  override def execute(args: Seq[String], context: CommandExecutionContext): CommandExecutionContext = {
    context.toFhirEngine.runningJobRegistry.getRunningExecutions() match {
      case m if m.isEmpty => println("There is no running task at the moment")
      case runningMappings => runningMappings.foreach(jobMappings => {
        println(s"Job: ${jobMappings._1}")
        jobMappings._2.foreach(executionMappings => {
          println(s"\tExecution: ${executionMappings._1}")
          executionMappings._2.foreach(mapping => println(s"\t\t$mapping"))
        })
      })
    }
    context
  }
}
