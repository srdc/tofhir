package io.tofhir.engine.mapping

import io.onfhir.api.Resource
import io.onfhir.api.service.{IFhirIdentityService, IFhirTerminologyService}
import io.tofhir.engine.model.{FhirInteraction, FhirMappingException}
import org.json4s.JsonAST.JObject

import scala.concurrent.Future

/**
 * Mapping service for a specific mapping definition (FhirMapping)
 */
trait IFhirMappingService extends Serializable {

  /**
   * For single source mappings, map the given source into one or more FHIR resources based on the underlying mapping definition for this service
   * @param source
   * @return  Mapping expression identifier, Mapped resources, optional FHIR interaction for persistence details
   */
  @throws[FhirMappingException]
  def mapToFhir(source:JObject):Future[Seq[(String, Seq[Resource], Option[FhirInteraction])]]

  /**
   * Map given source set into one or more FHIR resources based on the underlying mapping definition for this service
   * @param sources Map of source data (alis of the source in mapping definition FhirMapping.source.alias) -> Source object(s) as the input to the mapping
   * @return
   */
  @throws[FhirMappingException]
  def mapToFhir(sources:Map[String, Seq[JObject]]):Future[Seq[(String, Seq[Resource],Option[FhirInteraction])]]
}
