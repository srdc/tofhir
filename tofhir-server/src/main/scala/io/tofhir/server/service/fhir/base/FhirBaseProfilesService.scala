package io.tofhir.server.service.fhir.base

import io.onfhir.api.FHIR_FOUNDATION_RESOURCES.FHIR_STRUCTURE_DEFINITION
import io.onfhir.api.Resource
import io.onfhir.api.util.FHIRUtil
import io.onfhir.api.validation.ProfileRestrictions
import io.onfhir.config.{BaseFhirServerConfigurator, FSConfigReader}
import io.onfhir.r4.parsers.R4Parser
import io.tofhir.common.model.SchemaDefinition
import io.tofhir.engine.util.MajorFhirVersion
import io.tofhir.server.service.fhir.SimpleStructureDefinitionService

/**
 * Abstract class representing a service for managing FHIR base profiles.
 * Provides methods to retrieve resource types and profile schemas.
 */
abstract class FhirBaseProfilesService {
  /** A configuration reader for reading FHIR-related configuration. */
  val configReader: FSConfigReader
  /** A configurator for setting up the FHIR server platform. */
  val fhirServerConfigurator: BaseFhirServerConfigurator
  /** A sequence of base profile resources read from a configuration file. */
  private lazy val baseProfileResources = configReader.readStandardBundleFile("profiles-resources.json", Set(FHIR_STRUCTURE_DEFINITION))
  /** A sequence of resource types derived from base profile resources. */
  lazy val resourceTypes: Seq[String] = baseProfileResources.flatMap(getTypeFromStructureDefinition)
  /** A service to create SchemaDefinitions from the base profiles. */
  lazy val simpleStructureDefinitionService: SimpleStructureDefinitionService = new SimpleStructureDefinitionService(fhirServerConfigurator.initializePlatform(configReader))

  /**
   * Retrieves a sequence of schema definitions based on the given profile URLs.
   *
   * @param url A comma-separated string of profile URLs.
   * @return A sequence of schema definitions corresponding to the provided profile URLs.
   */
  def getProfile(url: String): Seq[SchemaDefinition] = {
    // split the provided URL string into a set of individual profile URLs
    val profileUrls = url.split(",").toSet

    baseProfileResources
      // filter the base profile resources to include only those with URLs matching the provided profile URLs
      .filter(p => profileUrls.contains(FHIRUtil.extractValueOption[String](p, "url").get))
      // map each filtered resource to a schema definition
      .map(resource => {
      // parse the structure definition from the resource
      val structureDefinition: ProfileRestrictions = new R4Parser().parseStructureDefinition(resource)
      // convert the parsed structure definition to a schema definition
      simpleStructureDefinitionService.convertToSchemaDefinition(structureDefinition)
    })
  }

  /**
   * Extracts the type from a given structure definition resource.
   *
   * @param structureDefinition The structure definition resource.
   * @return An optional string representing the resource type, or None if the resource is abstract.
   */
  private def getTypeFromStructureDefinition(structureDefinition: Resource): Option[String] = {
    if (FHIRUtil.extractValueOption[Boolean](structureDefinition, "abstract").get)
      None
    else
      Some(FHIRUtil.extractValueOption[String](structureDefinition, "type").get)
  }
}

/**
 * Companion object for FhirBaseProfilesService providing factory methods to create instances
 * based on the FHIR version.
 */
object FhirBaseProfilesService {
  /** Lazily initialized instance for FHIR R4 base profiles service. */
  lazy private val r4FhirBaseProfilesService = new R4FhirBaseProfilesService
  /** Lazily initialized instance for FHIR R5 base profiles service. */
  lazy private val r5FhirBaseProfilesService = new R5FhirBaseProfilesService

  /**
   * Factory method to create a FhirBaseProfilesService instance based on the FHIR version.
   *
   * @param fhirVersion The FHIR version (e.g., "R4", "R5").
   * @return An instance of FhirBaseProfilesService for the specified FHIR version.
   */
  def apply(fhirVersion: String): FhirBaseProfilesService = {
    fhirVersion match {
      case MajorFhirVersion.R4 => r4FhirBaseProfilesService
      case MajorFhirVersion.R5 => r5FhirBaseProfilesService
      case _ => throw new NotImplementedError()
    }
  }
}