package io.tofhir.server.model

import io.tofhir.server.util.JavaDateTimeSerializers
import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}

object JsonFormats {
  def getFormats: Formats = Serialization.formats(NoTypeHints) + JavaDateTimeSerializers.LocalDateTimeSerializer
}
