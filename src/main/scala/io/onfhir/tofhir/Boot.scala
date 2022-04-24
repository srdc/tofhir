package io.onfhir.tofhir

import io.onfhir.tofhir.cli.CommandLineInterface

/**
 * Entrypoint of toFHIR
 */
object Boot extends App {
  CommandLineInterface.start()
}
