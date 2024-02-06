package io.tofhir.common.util

import io.onfhir.api.Resource
import io.tofhir.common.model.{SchemaDefinition, SimpleStructureDefinition}
import org.json4s.JArray
import org.json4s.JsonDSL._
import io.tofhir.engine.config.ToFhirConfig

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
  def convertToStructureDefinitionResource(schemaDefinition: SchemaDefinition): Resource = {
    val fhirVersion = if (ToFhirConfig.engineConfig.fhirVersion == "R5") "5.0.0" else "4.0.1"
    val structureDefinitionResource: Resource =
      ("id" -> schemaDefinition.id) ~
        ("resourceType" -> "StructureDefinition") ~
        ("url" -> schemaDefinition.url) ~
        ("name" -> schemaDefinition.name) ~
        ("status" -> "draft") ~
        ("fhirVersion" -> fhirVersion) ~
        ("kind" -> "logical") ~
        ("abstract" -> false) ~
        ("type" -> schemaDefinition.`type`) ~
        ("baseDefinition" -> "http://hl7.org/fhir/StructureDefinition/Element") ~
        ("derivation" -> "specialization") ~
        ("differential" -> ("element" -> generateElementArray(schemaDefinition.`type`, schemaDefinition.fieldDefinitions.getOrElse(Seq.empty))))
    structureDefinitionResource
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
      if(fd.definition.nonEmpty){
        elementJson = elementJson ~ ("definition" -> fd.definition)
      }
      // add the field short if it exists
      if(fd.short.nonEmpty){
        elementJson = elementJson ~ ("short" -> fd.short)
      }
      elementJson
    }.toList

    JArray(rootElement +: elements)
  }
}
