package io.onfhir.tofhir.engine

import io.onfhir.tofhir.model.FhirMapping

trait IFhirMappingRepository {
  /**
   * Return the Fhir mapping definition by given url
   *
   * @param mappingUrl Fhir mapping url
   * @return
   */
  def getFhirMappingByUrl(mappingUrl: String): FhirMapping

}
