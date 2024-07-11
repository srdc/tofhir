package io.tofhir.engine.cli.command

trait Command {
  def execute(args: Seq[String], context: CommandExecutionContext): CommandExecutionContext
}


