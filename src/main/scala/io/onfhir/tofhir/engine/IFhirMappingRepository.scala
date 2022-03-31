package io.onfhir.tofhir.engine

import io.onfhir.tofhir.model.{FhirMapping, FhirMappingContext}

import scala.concurrent.Future

trait IFhirMappingRepository {
  /**
   * Return the Fhir mapping definition by given url
   * @param mappingUrl  Fhir mapping url
   * @return
   */
  def getFhirMappingByUrl(mappingUrl:String):Future[FhirMapping]

  /**
   * Return the Fhir mapping context data by given url
   * @return
   */
  def getFhirMappingContextByUrl:Future[FhirMappingContext]
}
