package io.tofhir.engine.cli

object CommandFactory {

  def apply(command: String): Command = {
    command match {
      case "help" => new Help()
      case "info" => new Info()
      case "load" => new Load()
      case "extract-redcap-schemas" => new ExtractRedCapSchemas()
      case "run" | "execute" => new Run()
      case "exit" | "quit" => new Exit()
      case cmd => new Unknown(cmd)
    }
  }

}
