package io.tofhir.engine.util

import java.time.{Instant, ZoneOffset}
import java.time.format.DateTimeFormatter

object TimeUtil {
  /**
   * Retrieves the current system time and formats it to ISO 8601 format in UTC.
   *
   * @return A string representing the current time in ISO 8601 format (e.g., "2024-10-18T11:02:18.399Z")
   */
  def getCurrentISOTime: String = {
    // Get current time in milliseconds
    val currentTimeMillis = System.currentTimeMillis()
    // Convert to Instant
    val instant = Instant.ofEpochMilli(currentTimeMillis)
    // Format the instant to ISO 8601 in UTC
    val formattedTime = DateTimeFormatter.ISO_INSTANT
      .withZone(ZoneOffset.UTC)
      .format(instant)
    formattedTime
  }
}
