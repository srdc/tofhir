package io.tofhir.engine.cli

class Help extends Command {
  override def execute(args: Seq[String], context: CommandExecutionContext): CommandExecutionContext = {
    println("List of available commands:\n" +
      "\tinfo - See info about the loaded Mapping Job.\n" +
      "\tload <path> - Load the Mapping Job definition file from the path.\n" +
      "\trun [<url>|<name>] - Run the task(s). Without a parameter, all task of the loaded Mapping Job are run. A specific task can be indicated with its name or URL.\n" +
      "\thelp - See the available commands and their use.\n" +
      "\tstop - Stop the execution of the MappingJob (if any).\n" +
      "\textract-redcap-schemas <path> <definition-root-url> - Extracts schemas from the given REDCap data dictionary file. Schemas will be annotated with the given definition root url.\n" +
      "\texit|quit - Exit the program.\n")
    context
  }
}
