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
 * @param variable   Common variables calculated from source data to use in the mappings
 * @param mapping     Mapping scripts
 */
case class FhirMapping(id: String = UUID.randomUUID().toString,
                       url: String,
                       name: String,
                       title: Option[String] = None,
                       description: Option[String] = None,
                       source: Seq[FhirMappingSource],
                       context: Map[String, FhirMappingContextDefinition],
                       variable:Seq[FhirExpression] = Nil,
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
 * Provides the details about FHIR interaction to persist the new information
 * @param `type`      FHIR interaction type e.g. create | update | patch
 *                    For FHIR patch interaction, the mapped content should be arranged accordingly (JSON patch or FHIR Path patch)
 * @param rlocator    Only required for FHIR Patch and provides either FHIR resource locator (e.g. Patient/ ) if required for the interaction (required for FHIR patch).
 *                    Note: Placeholders can be used to construct this
 *
 *                    e.g., Observation/{{resourceId}}
 *                    e.g. Observation/315653
 * @param condition   FHIR search statement indicating the condition to update or create (FHIR conditional create/update/patch).
 *                    For conditional patch, either rid or condition should be given
 *                    Note: Placeholders can be used to construct this
 *
 *                    e.g. Appending a new language to Patient resource --> language:not=en
 */
case class FhirInteraction(`type`:String, rid:Option[String] = None, condition:Option[String] = None)

/**
 * Mapping expression
 *
 * @param expression      FHIR expression that defines the mapping
 * @param precondition    A precondition FHIR expression for this mapping
 * @param fhirInteraction Provides information about the FHIR Interaction to persist the mapped information. If not given
 *                        FHIR Update interaction is used and the mapped content is expected to be a FHIR resource.
 */
case class FhirMappingExpression(expression: FhirExpression,
                                 precondition: Option[FhirExpression] = None,
                                 fhirInteraction: Option[FhirInteraction] = None
                                )
