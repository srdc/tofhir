package io.onfhir.tofhir.cli

trait Command {
  def execute(args: Seq[String], context: CommandExecutionContext): CommandExecutionContext
}


