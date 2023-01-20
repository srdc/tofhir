package io.tofhir.server.service.schema

import io.onfhir.api.Resource
import io.onfhir.api.parsers.IFhirFoundationResourceParser
import io.onfhir.api.validation.ProfileRestrictions
import io.onfhir.config.IFhirVersionConfigurator
import io.onfhir.r4.config.FhirR4Configurator
import io.onfhir.r4.parsers.R4Parser
import io.tofhir.server.model.{BadRequest, DataTypeWithProfiles, SchemaDefinition, SimpleStructureDefinition}
import io.tofhir.server.service.SimpleStructureDefinitionService
import org.json4s.JArray
import org.json4s.JsonDSL._

/**
 * Abstract class to provide common functionality to the implementations of ISchemaRepository
 *
 * @param fhirVersion
 */
abstract class AbstractSchemaRepository(fhirVersion: String = "R4") extends ISchemaRepository {

  /**
   * So that its validation function can be used when a new schema needs to be validated.
   */
  protected val fhirConfigurator: IFhirVersionConfigurator = fhirVersion match {
    case "R4" => new FhirR4Configurator()
    case _ => throw new NotImplementedError()
  }

  /**
   * So that a StructureDefinition resource can be parsed into ProfileRestrictions
   */
  protected val fhirFoundationResourceParser: IFhirFoundationResourceParser = fhirVersion match {
    case "R4" => new R4Parser()
    case _ => throw new NotImplementedError()
  }

  /**
   * Convert our internal SchemaDefinition instance to FHIR StructureDefinition resource
   * @param schemaDefinition
   * @return
   */
  protected def convertToStructureDefinitionResource(schemaDefinition: SchemaDefinition): Resource = {
    val structureDefinitionResource: Resource =
      ("resourceType" -> "StructureDefinition") ~
        ("url" -> schemaDefinition.url) ~
        ("name" -> schemaDefinition.name) ~
        ("status" -> "draft") ~
        ("fhirVersion" -> "4.0.1") ~
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
   * @param `type`
   * @param fieldDefinitions
   * @return
   */
  private def generateElementArray(`type`: String, fieldDefinitions: Seq[SimpleStructureDefinition]): JArray = {
    // Check whether all field definitions have at least one data type
    val integrityCheck = fieldDefinitions.forall(fd => fd.dataTypes.isDefined && fd.dataTypes.get.nonEmpty)
    if (!integrityCheck) {
      throw BadRequest("Missing data type.", s"A field definition must have at least one data type. Element rootPath: ${`type`}")
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
      ("id" -> fd.path) ~
        ("path" -> fd.path) ~
        ("short" -> fd.short) ~
        ("definition" -> fd.definition) ~
        ("min" -> fd.minCardinality) ~
        ("max" -> max) ~
        ("type" -> fd.dataTypes.get.map { dt =>
          ("code" -> dt.dataType) ~
            ("profile" -> dt.profiles)
        })
    }.toList

    JArray(rootElement +: elements)
  }

  /**
   * Convert ProfileRestrictions into a SchemaDefinition instance.
   * @param profileRestrictions
   * @param simpleStructureDefinitionService
   * @return
   */
  protected def convertToSchemaDefinition(profileRestrictions: ProfileRestrictions, simpleStructureDefinitionService: SimpleStructureDefinitionService): SchemaDefinition = {
    val rootElementDefinition = createRootElement(profileRestrictions.resourceType)
    SchemaDefinition(url = profileRestrictions.url,
      `type` = profileRestrictions.resourceType,
      name = profileRestrictions.resourceName.getOrElse(profileRestrictions.resourceType),
      rootDefinition = Some(rootElementDefinition),
      fieldDefinitions = Some(simpleStructureDefinitionService.simplifyStructureDefinition(profileRestrictions.url, withResourceTypeInPaths = true)))
  }

  /**
   * Helper function to create the root element (1st element of the Element definitions which is dropped by IFhirFoundationParser.
   * @param resourceType
   * @return
   */
  private def createRootElement(resourceType: String): SimpleStructureDefinition = {
    SimpleStructureDefinition(
      id = resourceType,
      path = resourceType,
      dataTypes = Some(Seq(DataTypeWithProfiles("Element", None))),
      isPrimitive = false,
      isChoiceRoot = false,
      isArray = false,
      minCardinality = 0, maxCardinality = None,
      boundToValueSet = None,
      isValueSetBindingRequired = None,
      referencableProfiles = None,
      constraintDefinitions = None,
      sliceDefinition = None,
      sliceName = None,
      fixedValue = None, patternValue = None,
      referringTo = None,
      short = None,
      definition = None,
      comment = None,
      elements = None)
  }

}
