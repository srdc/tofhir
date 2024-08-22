package io.tofhir.common.model

import io.tofhir.common.util.HashUtil
import org.json4s.JsonAST.{JObject, JString}

/**
 * Entity representing a FHIR StructureDefinition in the context of toFHIR
 *
 * @param id               Identifier of the schema
 * @param url              URL of the schema
 * @param `type`           Type of entities that this schema represents
 * @param name             Name of the schema
 * @param description      Description of the schema
 * @param rootDefinition   Root element definition for the schema i.e. the first element in the definition
 * @param fieldDefinitions Rest of the element definitions
 */
case class SchemaDefinition(id: String,
                            url: String,
                            `type`: String,
                            name: String,
                            description: Option[String],
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

object SchemaDefinition {
  def apply(url: String,
            `type`: String,
            name: String,
            description: Option[String],
            rootDefinition: Option[SimpleStructureDefinition],
            fieldDefinitions: Option[Seq[SimpleStructureDefinition]]): SchemaDefinition = {
    // If id is not provided, use MD5 hash of url as the id
    val effectiveId = HashUtil.md5Hash(url)
    SchemaDefinition(effectiveId, url, `type`, name, description, rootDefinition, fieldDefinitions)
  }
}