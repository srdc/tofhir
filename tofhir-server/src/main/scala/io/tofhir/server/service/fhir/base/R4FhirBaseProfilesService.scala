package io.tofhir.server.service.fhir.base

import io.onfhir.config.{BaseFhirServerConfigurator, FSConfigReader}
import io.onfhir.r4.config.FhirR4Configurator
import io.tofhir.engine.util.MajorFhirVersion

/**
 * Implementation of FhirBaseProfilesService for FHIR R4.
 */
class R4FhirBaseProfilesService extends FhirBaseProfilesService {
  override val configReader: FSConfigReader = new FSConfigReader(fhirVersion = MajorFhirVersion.R4)
  override val fhirServerConfigurator: BaseFhirServerConfigurator = new FhirR4Configurator()
}
