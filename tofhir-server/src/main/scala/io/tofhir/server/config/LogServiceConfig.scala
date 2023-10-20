package io.tofhir.server.config

import com.typesafe.config.Config

import scala.util.Try

class LogServiceConfig(logServiceConfig: Config) {
  /** Host name/address to start service on. */
  lazy val logServiceEndpoint: String = Try(logServiceConfig.getString("endpoint")).getOrElse("http://localhost:8086/tofhir-log")
}
