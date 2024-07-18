package io.tofhir.server.service.fhir.base

import io.onfhir.config.{BaseFhirServerConfigurator, FSConfigReader}
import io.onfhir.r5.config.FhirR5Configurator
import io.tofhir.engine.util.MajorFhirVersion

/**
 * Implementation of FhirBaseProfilesService for FHIR R5.
 */
class R5FhirBaseProfilesService extends FhirBaseProfilesService {
  override val configReader: FSConfigReader = new FSConfigReader(fhirVersion = MajorFhirVersion.R5)
  override val fhirServerConfigurator: BaseFhirServerConfigurator = new FhirR5Configurator()
}
