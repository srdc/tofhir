package io.tofhir.server

import com.typesafe.scalalogging.Logger
import io.tofhir.common.app.AppVersion

object Boot extends App {
  val logger: Logger = Logger(this.getClass)
  logger.info(s"Starting toFHIR version: ${AppVersion.getVersion}")

  ToFhirServer.start()
}
