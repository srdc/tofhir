package io.tofhir.engine.cli.command

import scala.io.StdIn

class Exit extends Command {
  override def execute(args: Seq[String], context: CommandExecutionContext): CommandExecutionContext = {
    if(context.toFhirEngine.runningJobRegistry.getRunningExecutions().nonEmpty) {
      val line = StdIn.readLine("I am still running a mapping job/task. Do you still want me to exit? (Y/n)\n")
      if (line.equalsIgnoreCase("") || line.equalsIgnoreCase("Y")) {
        System.exit(0)
      } else if (line.equalsIgnoreCase("N")) {
        // Do nothing...
      } else {
        println(s"Unknown input: $line")
      }
      context
    } else {
      System.exit(0)
      context
    }
  }
}
