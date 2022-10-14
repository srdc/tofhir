package io.tofhir.engine.cli

trait Command {
  def execute(args: Seq[String], context: CommandExecutionContext): CommandExecutionContext
}


