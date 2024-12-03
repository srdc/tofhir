package io.tofhir.server.config

import com.typesafe.config.Config

import scala.util.Try

/**
 * Configuration class for the tofhir-redcap.
 *
 * @param redCapServiceConfig The configuration object containing the settings for the tofhir-redcap service.
 */
class RedCapServiceConfig(redCapServiceConfig: Config) {
  /**
   * The path to the notification endpoint.
   */
  private lazy val notificationPath: String = Try(redCapServiceConfig.getString("paths.notification")).getOrElse("notification")

  /**
   * The path to the projects endpoint.
   */
  private lazy val projectsPath: String = Try(redCapServiceConfig.getString("paths.projects")).getOrElse("projects")

  /**
   * The path to the project's data endpoint
   * */
  lazy val projectDataPath: String = Try(redCapServiceConfig.getString("paths.projectData")).getOrElse("data")
  /**
   * Parameter to reload REDCap data upon recreation of Kafka topics.
   * */
  lazy val projectDataReloadParameter: String = Try(redCapServiceConfig.getString("parameters.reload")).getOrElse("reload")

  /**
   * The base endpoint URL for the tofhir-redcap service.
   */
  lazy val endpoint: String = Try(redCapServiceConfig.getString("endpoint")).getOrElse("http://localhost:8095/tofhir-redcap")

  /**
   * Constructs the full URL for the notification endpoint by combining the base endpoint URL and the notification path.
   */
  lazy val notificationEndpoint: String = s"$endpoint/$notificationPath"

  /**
   * Constructs the full URL for the projects endpoint by combining the base endpoint URL and the projects path.
   */
  lazy val projectsEndpoint: String = s"$endpoint/$projectsPath"
}
