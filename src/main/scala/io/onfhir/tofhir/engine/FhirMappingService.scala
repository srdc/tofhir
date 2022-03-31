package io.onfhir.tofhir.engine

import io.onfhir.template.FhirTemplateExpressionHandler
import io.onfhir.tofhir.model.{ConfigurationContext, FhirMappingContext, FhirMappingExpression}
import org.json4s.JsonAST.JObject

/**
 * Mapping service for a specific FhirMapping together with contextual data and mapping scripts
 * @param sources   List of source aliases
 * @param context   Context data
 * @param mappings  Mapping scripts
 */
class FhirMappingService(
                          sources:Seq[String],
                          context:Map[String, FhirMappingContext],
                          mappings:Seq[FhirMappingExpression]
                        ) extends IFhirMappingService {

  /**
   * Template expression handler that will perform the mapping by executing the placeholder expressions
   */
  val templateEngine =
    new FhirTemplateExpressionHandler(
      context.filter(_._2.isInstanceOf[ConfigurationContext]).map(c => c._1 -> c._2.toContextObject), // Provide the static contexts
      Map("mpp" -> new FhirMappingFunctionsFactory(context.filterNot(_._2.isInstanceOf[ConfigurationContext]))) //Add our mapping function library
    )

  /**
   * For single source mappings, map the given source into one or more FHIR resources based on the underlying mapping definition for this service
   *
   * @param source
   * @return
   */
  override def mapToFhir(source: JObject): Seq[JObject] = ???

  /**
   * Map given source set into one or more FHIR resources based on the underlying mapping definition for this service
   *
   * @param sources Map of source data (alis of the source in mapping definition FhirMapping.source.alias) -> Source object(s) as the input to the mapping
   * @return
   */
  override def mapToFhir(sources: Map[String, Seq[JObject]]): Seq[JObject] = ???
}
