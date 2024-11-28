package io.tofhir.engine.repository.mapping

import io.tofhir.engine.model.FhirMapping
import io.tofhir.engine.repository.ICachedRepository

trait IFhirMappingRepository extends ICachedRepository {
  /**
   * Return the Fhir mapping definition by given url
   *
   * @param mappingUrl Fhir mapping url
   * @return
   */
  def getFhirMappingByUrl(mappingUrl: String): FhirMapping

}
