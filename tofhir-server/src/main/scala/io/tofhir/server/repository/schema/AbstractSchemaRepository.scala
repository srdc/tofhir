package io.tofhir.server.repository.schema

import io.onfhir.api.parsers.IFhirFoundationResourceParser
import io.onfhir.config.IFhirVersionConfigurator
import io.onfhir.r4.config.FhirR4Configurator
import io.onfhir.r4.parsers.R4Parser
import io.onfhir.r5.config.FhirR5Configurator
import io.tofhir.engine.config.ToFhirConfig
import io.tofhir.engine.util.{FhirVersionUtil, MajorFhirVersion}

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
}
