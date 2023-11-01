package io.tofhir.engine.util

import io.tofhir.engine.model.KafkaSourceSettings
import io.tofhir.engine.util.FhirMappingJobFormatter.formats
import org.json4s.JsonAST.JString
import org.json4s._

object KafkaSourceSettingsSerializers {
  /**
   * Custom serializer for [[KafkaSourceSettings]] to include asStream property in the serialized JSON.
   */
  case object KafkaSourceSettingsSerializer extends CustomSerializer[KafkaSourceSettings](format => (
  {
    // deserialize
    case json: JObject =>
      val name = (json \ "name").extractOrElse("Unknown")
      val sourceUri = (json \ "sourceUri").extractOrElse("Unknown")
      val bootstrapServers = (json \ "bootstrapServers").extractOrElse("Unknown")

      KafkaSourceSettings(name, sourceUri, bootstrapServers)
  },
  {
    // serialize
    case settings: KafkaSourceSettings =>
      JObject(
        "name" -> JString(settings.name),
        "sourceUri" -> JString(settings.sourceUri),
        "bootstrapServers" -> JString(settings.bootstrapServers),
        "asStream" -> JBool(settings.asStream),
        "jsonClass" -> JString("KafkaSourceSettings")
      )
  }
  ))

}
