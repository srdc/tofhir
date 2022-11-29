package io.tofhir.server.service

import io.onfhir.api.FHIR_ROOT_URL_FOR_DEFINITIONS
import io.onfhir.config.BaseFhirConfig
import io.onfhir.r4.config.FhirR4Configurator
import io.tofhir.server.fhir.{FhirDefinitionsConfig, FhirEndpointResourceReader}
import io.tofhir.server.model.SimpleStructureDefinition

import scala.collection.mutable

class FhirDefinitionsService(fhirDefinitionsConfig: FhirDefinitionsConfig) {

  val fhirConfigReader = new FhirEndpointResourceReader(fhirDefinitionsConfig)
  val baseFhirConfig: BaseFhirConfig = new FhirR4Configurator().initializePlatform(fhirConfigReader)
  val simpleStructureDefinitionService = new SimpleStructureDefinitionService(baseFhirConfig)

  val profilesCache: mutable.Map[String, Set[String]] = mutable.HashMap()
  val simplifiedStructureDefinitionCache: mutable.Map[String, Seq[SimpleStructureDefinition]] = mutable.HashMap()

  /**
   *
   * @return
   */
  def getResourceTypes(): Set[String] = {
    baseFhirConfig.FHIR_RESOURCE_TYPES
  }

  /**
   *
   * @param rtype
   * @return
   */
  def getProfilesFor(rtype: String): Set[String] = {
    val resourceUrl = s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/$rtype"
    profilesCache.getOrElseUpdate(resourceUrl, getProfilesForUrl(resourceUrl, Set.empty[String]))
  }

  /**
   *
   * @param url
   * @param set
   * @return
   */
  private def getProfilesForUrl(url: String, set: Set[String]): Set[String] = {
    val foundProfiles = baseFhirConfig.profileRestrictions
      .filter(e => e._2.baseUrl.isDefined && e._2.baseUrl.get == url).keySet
    if(foundProfiles.isEmpty || foundProfiles.diff(set).isEmpty) {
      set
    }
    else {
      foundProfiles
        .map(purl => getProfilesForUrl(purl, set ++ foundProfiles))
        .reduce((s1, s2) => s1 ++ s2)
    }
  }

  /**
   *
   * @param profileUrl
   * @return
   */
  def getElementDefinitionsOfProfile(profileUrl: String): Seq[SimpleStructureDefinition] = {
    simplifiedStructureDefinitionCache.getOrElseUpdate(profileUrl, simpleStructureDefinitionService.simplifyStructureDefinition(profileUrl))
  }

}
