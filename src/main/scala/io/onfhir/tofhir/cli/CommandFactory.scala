package io.onfhir.tofhir.cli

object CommandFactory {

  def apply(command: String): Command = {
    command match {
      case "help" => new Help()
      case "info" => new Info()
      case "load" => new Load()
      case "run" => new Run()
      case cmd => new Unknown(cmd)
    }
  }

}
