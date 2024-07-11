package io.tofhir.engine.cli.command

object CommandFactory {

  def apply(command: String): Command = {
    command match {
      case "help" => new Help()
      case "load" => new Load()
      case "extract-redcap-schemas" => new ExtractRedCapSchemas()
      case "reload" => new Reload()
      case "run" | "execute" => new Run()
      case "list" => new ListRunningMappings()
      case "stop" => new Stop()
      case "exit" | "quit" => new Exit()
      case cmd => new Unknown(cmd)
    }
  }

}
