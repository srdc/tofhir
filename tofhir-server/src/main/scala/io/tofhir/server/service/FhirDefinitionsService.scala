package io.tofhir.server.service

import io.onfhir.r4.config.FhirR4Configurator
import io.tofhir.server.fhir.{FhirDefinitionsConfig, FhirEndpointResourceReader}

class FhirDefinitionsService(fhirDefinitionsConfig: FhirDefinitionsConfig) {

  def getResourceTypes(): Set[String] = {
    val fhirConfigReader = new FhirEndpointResourceReader(fhirDefinitionsConfig)
    val baseFhirConfig = new FhirR4Configurator().initializePlatform(fhirConfigReader)
    baseFhirConfig.FHIR_RESOURCE_TYPES
  }

}
