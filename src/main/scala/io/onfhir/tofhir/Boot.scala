package io.onfhir.tofhir

import io.onfhir.tofhir.config.ToFhirConfig

/**
 * Entrypoint of toFHIR
 */
object Boot extends App {
  println(ToFhirConfig.appName)
}
