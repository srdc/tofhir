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
      ("kind" -> "resource") ~
      ("abstract" -> false) ~
      ("type" -> schemaDefinition.`type`) ~
      ("baseDefinition" -> "http://hl7.org/fhir/StructureDefinition/Element") ~
      ("derivation" -> "constraint") ~
      ("differential" -> ("element" -> generateElementArray(schemaDefinition.id, schemaDefinition.`type`, schemaDefinition.fieldDefinitions.getOrElse(Seq.empty))))
  }

  /**
   * Helper function to convertToStructureDefinitionResource to convert each field definition.
   *
   * @param `type`           type of the schema
   * @param fieldDefinitions fields of the schema
   * @return
   * @throws IllegalArgumentException if a field definition does not have at least one data type
   */
  private def generateElementArray(id: String, `type`: String, fieldDefinitions: Seq[SimpleStructureDefinition]): JArray = {

    // Normalize path so that it starts with the schema type
    def normalizePath(p: String): String = {
      val raw = Option(p).map(_.trim).getOrElse("")
      val clean =
        if (id != null && id.nonEmpty && (raw == id || raw.startsWith(id + ".")))
        raw.stripPrefix(id + ".").stripPrefix(id)
      else raw
      val t = `type`
      if (clean.isEmpty) t
      else if (clean == t || clean.startsWith(t + ".")) clean
      else t + "." + clean
    }

    // Flatten nested elements into a single list suitable for StructureDefinition.differential.element
    def flatten(defs: Seq[SimpleStructureDefinition]): Seq[SimpleStructureDefinition] = {
      defs.flatMap { fd =>
        val np = normalizePath(fd.path)
        val self = fd.copy(
          id       = np,
          path     = np,
          elements = None
        )
        self +: flatten(fd.elements.getOrElse(Seq.empty))
      }
    }

    val flatFds = flatten(fieldDefinitions)

    // Validate types after flatten (defensive — we rely on types to build a correct SD)
    val integrityCheck = flatFds.forall(fd => fd.dataTypes.isDefined && fd.dataTypes.get.nonEmpty)
    if (!integrityCheck) {
      throw new IllegalArgumentException(s"Missing data type.A field definition must have at least one data type. Element rootPath: ${`type`}")
    }

    val rootElement =
      ("id" -> `type`) ~
        ("path" -> `type`)

    // Children (flat list)
    val elements = flatFds.map { fd =>
      val max: String = fd.maxCardinality match {
        case Some(v) => v.toString
        case None    => if (fd.isArray) "*" else "1"
      }
      // create element json
      var elementJson =
        ("id" -> fd.path) ~
          ("path" -> fd.path) ~
          ("min" -> fd.minCardinality) ~
          ("max" -> max) ~
          ("type" -> JArray(fd.dataTypes.get.map { dt =>
            ("code" -> dt.dataType) ~
              ("profile" -> dt.profiles)
          }.toList))
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

  /**
   * Helper to check if a schema's field definitions have elements in them
   * @param schema Schema to be classified
   * @return
   */
  def isDeepSchema(schema: SchemaDefinition): Boolean = {
    schema.fieldDefinitions
      .getOrElse(Seq.empty)
      .exists(fd => fd.elements.exists(_.nonEmpty))
  }
}
