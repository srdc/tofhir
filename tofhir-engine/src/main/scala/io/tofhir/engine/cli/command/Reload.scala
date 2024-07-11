package io.tofhir.engine.cli.command

class Reload extends Command {
  override def execute(args: Seq[String], context: CommandExecutionContext): CommandExecutionContext = {
    context.toFhirEngine.mappingRepo.invalidate()
    println("Fhir mapping definitions have been reloaded.")
    context
  }
}
