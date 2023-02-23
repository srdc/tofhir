package io.tofhir.server.model

import org.json4s.JsonAST.{JObject, JString}

import java.util.UUID

/**
 * Entity representing a FHIR StructureDefinition in the context of toFHIR
 *
 * @param id               Identifier of the schema
 * @param url              URL of the schema
 * @param `type`           Type of entities that this schema represents
 * @param name             Name of the schema
 * @param rootDefinition   Root element definition for the schema i.e. the first element in the definition
 * @param fieldDefinitions Rest of the element definitions
 */
case class SchemaDefinition(id: String = UUID.randomUUID().toString,
                            url: String,
                            `type`: String,
                            name: String,
                            rootDefinition: Option[SimpleStructureDefinition],
                            fieldDefinitions: Option[Seq[SimpleStructureDefinition]]) {


  /**
   * Retrieves metadata for this schema. The metadata is being used in the folder-based implementation for the time being.
   *
   * @return
   */
  def getMetadata(): JObject = {
    JObject(
      List(
        "id" -> JString(this.id),
        "url" -> JString(this.url),
        "type" -> JString(this.`type`),
        "name" -> JString(this.name)
      )
    )
  }
}
