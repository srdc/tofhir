package io.tofhir.engine.model

import io.onfhir.expression.FhirExpression
import org.json4s.JsonAST.JObject

import java.util.UUID

/**
 * Definition of mapping from a source format to FHIR
 *
 * @param id          Unique identifier for the mapping
 * @param url         Canonical url for the mapping
 * @param name        Computer friendly name
 * @param title       Human friendly title
 * @param description Description of the mapping
 * @param source      Metadata about source for the mapping
 * @param context     Further context to use for mapping evaluation e.g. ConceptMap for terminology mapping, definition of unit conversion functions
 * @param mapping     Mapping scripts
 */
case class FhirMapping(id: String = UUID.randomUUID().toString,
                       url: String,
                       name: String,
                       title: Option[String] = None,
                       description: Option[String] = None,
                       source: Seq[FhirMappingSource],
                       context: Map[String, FhirMappingContextDefinition],
                       mapping: Seq[FhirMappingExpression]
                      ) {
  def withContext(newContext: Map[String, FhirMappingContextDefinition]): FhirMapping = {
    this.copy(context = newContext)
  }
}

/**
 * Metadata definition for the source data
 *
 * @param alias       Name of the source to be used in expressions (First source will always be used as main input for expression evaluations)
 * @param url         URL to the StructureDefinition of the source format for validation and deserialization purposes
 * @param description Description of the source
 */
case class FhirMappingSource(alias: String, url: String, description: Option[String] = None)

/**
 * Context information for mapping evaluation
 *
 * @param category    Category of context see [[FhirMappingContextCategories]]
 * @param url         If context data will be loaded from a URL, URL itself e.g. a file path for concept map csv
 * @param value       If context data will be supplied as JSON content
 * @param description Description of the context data
 */
case class FhirMappingContextDefinition(category: String, url: Option[String], value: Option[JObject], description: Option[String] = None) {
  def withURL(newURL: String): FhirMappingContextDefinition = {
    this.copy(url = Some(newURL))
  }
}

object FhirMappingContextCategories {
  final val CONCEPT_MAP = "concept-map"
  final val UNIT_CONVERSION_FUNCTIONS = "unit-conversion"
}

/**
 * Mapping expression
 *
 * @param expression   FHIR expression that defines the mapping
 * @param precondition A precondition FHIR expression for this mapping
 */
case class FhirMappingExpression(expression: FhirExpression, precondition: Option[FhirExpression] = None)
