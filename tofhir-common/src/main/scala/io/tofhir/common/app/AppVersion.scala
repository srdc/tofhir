package io.tofhir.common.app

import com.typesafe.config.ConfigFactory

import scala.util.Try

/**
 * Helper function to read properties file to retrieve the application version.
 * We configure Maven in such a way that it replaces the application.version in the properties file
 * with the project.version set in pom.xml.
 */
object AppVersion {

  private val PROPERTIES_FILE_NAME = "version.properties"

  /**
   * @return The version of the application as String
   */
  def getVersion: String = {
    val config = ConfigFactory.load(PROPERTIES_FILE_NAME)
    Try(config.getString("application.version")).getOrElse("UNKNOWN")
  }
}
