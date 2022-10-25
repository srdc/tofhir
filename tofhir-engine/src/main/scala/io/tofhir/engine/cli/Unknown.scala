package io.tofhir.engine.cli

class Unknown(cmd: String) extends Command {
  override def execute(args: Seq[String], context: CommandExecutionContext): CommandExecutionContext = {
    println(s"Unknown command: $cmd")
    new Help().execute(args, context)
  }
}
