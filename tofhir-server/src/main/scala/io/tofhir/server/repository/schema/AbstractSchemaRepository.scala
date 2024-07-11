package io.tofhir.server.repository.schema

import io.onfhir.api.parsers.IFhirFoundationResourceParser
import io.onfhir.api.validation.ProfileRestrictions
import io.onfhir.config.IFhirVersionConfigurator
import io.onfhir.r4.config.FhirR4Configurator
import io.onfhir.r4.parsers.R4Parser
import io.onfhir.r5.config.FhirR5Configurator
import io.tofhir.common.model.{DataTypeWithProfiles, SchemaDefinition, SimpleStructureDefinition}
import io.tofhir.engine.config.ToFhirConfig
import io.tofhir.engine.util.{FhirVersionUtil, MajorFhirVersion}
import io.tofhir.server.service.fhir.SimpleStructureDefinitionService

/**
 * Abstract class to provide common functionality to the implementations of ISchemaRepository
 */
abstract class AbstractSchemaRepository extends ISchemaRepository {

  /**
   * So that its validation function can be used when a new schema needs to be validated.
   */
  protected val fhirConfigurator: IFhirVersionConfigurator = FhirVersionUtil.getMajorFhirVersion(ToFhirConfig.engineConfig.schemaRepositoryFhirVersion) match {
    case MajorFhirVersion.R4 => new FhirR4Configurator()
    case MajorFhirVersion.R5 => new FhirR5Configurator()
    case _ => throw new NotImplementedError()
  }

  /**
   * So that a StructureDefinition resource can be parsed into ProfileRestrictions
   */
  protected val fhirFoundationResourceParser: IFhirFoundationResourceParser = FhirVersionUtil.getMajorFhirVersion(ToFhirConfig.engineConfig.schemaRepositoryFhirVersion) match {
    case MajorFhirVersion.R4 => new R4Parser()
    case MajorFhirVersion.R5 => new R4Parser()
    case _ => throw new NotImplementedError()
  }

  /**
   * Convert ProfileRestrictions into a SchemaDefinition instance.
   *
   * @param profileRestrictions
   * @param id
   * @param project
   * @param simpleStructureDefinitionService
   * @return
   */
  protected def convertToSchemaDefinition(profileRestrictions: ProfileRestrictions, simpleStructureDefinitionService: SimpleStructureDefinitionService): SchemaDefinition = {
    val rootElementDefinition = createRootElement(profileRestrictions.resourceType)
    SchemaDefinition(id = profileRestrictions.id.getOrElse(profileRestrictions.resourceType),
      url = profileRestrictions.url,
      `type` = profileRestrictions.resourceType,
      name = profileRestrictions.resourceName.getOrElse(profileRestrictions.resourceType),
      rootDefinition = Some(rootElementDefinition),
      fieldDefinitions = Some(simpleStructureDefinitionService.simplifyStructureDefinition(profileRestrictions.url, withResourceTypeInPaths = true)))
  }

  /**
   * Helper function to create the root element (1st element of the Element definitions which is dropped by IFhirFoundationParser.
   *
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
