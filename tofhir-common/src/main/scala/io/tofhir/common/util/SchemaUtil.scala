package io.tofhir.common.util

import io.onfhir.api.Resource
import io.onfhir.definitions.common.model.{SchemaDefinition, SimpleStructureDefinition}
import org.json4s.JArray
import org.json4s.JsonDSL._

/**
 * Utility class providing a function to convert a {@link SchemaDefinition} to {@link Resource}
 * */
object SchemaUtil {
  /**
   * Convert our internal SchemaDefinition instance to FHIR StructureDefinition resource
   *
   * @param schemaDefinition the schema
   * @return
   */
  def convertToStructureDefinitionResource(schemaDefinition: SchemaDefinition, fhirVersion: String): Resource = {
    var structureDefinitionResource: Resource =
      ("id" -> schemaDefinition.id) ~
        ("resourceType" -> "StructureDefinition") ~
        ("url" -> schemaDefinition.url) ~
        ("version" -> schemaDefinition.version) ~
        ("name" -> schemaDefinition.name)
    if (schemaDefinition.description.isDefined) { // If the description exists, add it here in order not to break the order of JSON elements (better to see the description close to the name)
      structureDefinitionResource = structureDefinitionResource ~ ("description" -> schemaDefinition.description.get)
    }
    structureDefinitionResource ~
      ("status" -> "draft") ~
      ("fhirVersion" -> fhirVersion) ~
      ("kind" -> "logical") ~
      ("abstract" -> false) ~
      ("type" -> schemaDefinition.`type`) ~
      ("baseDefinition" -> "http://hl7.org/fhir/StructureDefinition/Element") ~
      ("derivation" -> "specialization") ~
      ("differential" -> ("element" -> generateElementArray(schemaDefinition.`type`, schemaDefinition.fieldDefinitions.getOrElse(Seq.empty))))
  }

  /**
   * Helper function to convertToStructureDefinitionResource to convert each field definition.
   *
   * @param `type`           type of the schema
   * @param fieldDefinitions fields of the schema
   * @return
   * @throws IllegalArgumentException if a field definition does not have at least one data type
   */
  private def generateElementArray(`type`: String, fieldDefinitions: Seq[SimpleStructureDefinition]): JArray = {
    // Check whether all field definitions have at least one data type
    val integrityCheck = fieldDefinitions.forall(fd => fd.dataTypes.isDefined && fd.dataTypes.get.nonEmpty)
    if (!integrityCheck) {
      throw new IllegalArgumentException(s"Missing data type.A field definition must have at least one data type. Element rootPath: ${`type`}")
    }

    val rootElement =
      ("id" -> `type`) ~
        ("path" -> `type`) ~
        ("min" -> 0) ~
        ("max" -> "*") ~
        ("type" -> JArray(List("code" -> "Element")))

    val elements = fieldDefinitions.map { fd =>
      val max: String = fd.maxCardinality match {
        case Some(v) => v.toString
        case None => "*"
      }
      // create element json
      var elementJson =
        ("id" -> fd.path) ~
          ("path" -> fd.path) ~
          ("min" -> fd.minCardinality) ~
          ("max" -> max) ~
          ("type" -> fd.dataTypes.get.map { dt =>
            ("code" -> dt.dataType) ~
              ("profile" -> dt.profiles)
          })
      // add the field definition if it exists
      if (fd.definition.nonEmpty) {
        elementJson = elementJson ~ ("definition" -> fd.definition)
      }
      // add the field short if it exists
      if (fd.short.nonEmpty) {
        elementJson = elementJson ~ ("short" -> fd.short)
      }
      elementJson
    }.toList

    JArray(rootElement +: elements)
  }
}
