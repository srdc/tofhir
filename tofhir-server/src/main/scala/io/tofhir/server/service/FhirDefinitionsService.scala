package io.tofhir.server.service

import com.typesafe.scalalogging.Logger
import io.onfhir.api
import io.onfhir.config.{BaseFhirConfig, FSConfigReader, IFhirConfigReader}
import io.onfhir.r4.config.FhirR4Configurator
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.fhir.{FhirDefinitionsConfig, FhirEndpointResourceReader}
import io.tofhir.common.model.SimpleStructureDefinition

import scala.collection.mutable

class FhirDefinitionsService(fhirDefinitionsConfig: FhirDefinitionsConfig) {

  private val logger: Logger = Logger(this.getClass)

  val fhirConfigReader: IFhirConfigReader = fhirDefinitionsConfig.definitionsFHIREndpoint match {
    case Some(_) =>
      new FhirEndpointResourceReader(fhirDefinitionsConfig)
    case None =>
      // use the local file system, create Option[String] from Option[File] if file exists or None if not
      val profilesPath = fhirDefinitionsConfig.profilesPath.flatMap(path => {
        val filePath = FileUtils.getPath(path)
        if (filePath.toFile.exists())
          Some(filePath.toString)
        else {
          logger.warn("Profiles folder path is configured but folder does not exist.")
          None
        }
      })

      val codeSystemsPath = fhirDefinitionsConfig.codesystemsPath.flatMap(path => {
        val filePath = FileUtils.getPath(path)
        if (filePath.toFile.exists())
          Some(filePath.toString)
        else {
          logger.warn("CodeSystems folder path is configured but folder does not exist.")
          None
        }
      })

      val valueSetsPath = fhirDefinitionsConfig.valuesetsPath.flatMap(path => {
        val filePath = FileUtils.getPath(path)
        if (filePath.toFile.exists())
          Some(filePath.toString)
        else {
          logger.warn("ValueSets folder path is configured but folder does not exist.")
          None
        }
      })

      new FSConfigReader(profilesPath = profilesPath, codeSystemsPath = codeSystemsPath, valueSetsPath = valueSetsPath)
  }

  val baseFhirConfig: BaseFhirConfig = new FhirR4Configurator().initializePlatform(fhirConfigReader)
  val simpleStructureDefinitionService = new SimpleStructureDefinitionService(baseFhirConfig)

  val profilesCache: mutable.Map[String, Set[String]] = mutable.HashMap()
  val simplifiedStructureDefinitionCache: mutable.Map[String, Seq[SimpleStructureDefinition]] = mutable.HashMap()

  /**
   * Get all available FHIR resource types.
   *
   * @return
   */
  def getResourceTypes(): Set[String] = {
    baseFhirConfig.FHIR_RESOURCE_TYPES
  }

  /**
   * Given a resource type (e.g., Observation), get the profile URLs whose type are that rtype.
   *
   * @param rtype Resource type (e.g., Condition)
   * @return
   */
  def getProfilesFor(rtype: String): Set[String] = {
    val resourceUrl = s"${api.FHIR_ROOT_URL_FOR_DEFINITIONS}/StructureDefinition/$rtype"
    profilesCache.getOrElseUpdate(resourceUrl, getProfilesForUrl(resourceUrl, Set.empty[String]))
  }

  /**
   * Helper function to recursively find the profiles defined based on a given profile.
   *
   * @param url
   * @param set
   * @return
   */
  private def getProfilesForUrl(url: String, set: Set[String]): Set[String] = {
    val foundProfiles = baseFhirConfig.profileRestrictions
      .filter(e => e._2.baseUrl.isDefined && e._2.baseUrl.get == url).keySet
    if (foundProfiles.isEmpty || foundProfiles.diff(set).isEmpty) {
      set
    }
    else {
      foundProfiles
        .map(purl => getProfilesForUrl(purl, set ++ foundProfiles))
        .reduce((s1, s2) => s1 ++ s2)
    }
  }

  /**
   * Given a URL for a profile, retrieve the simplified structure definition of that profile.
   *
   * @param profileUrl
   * @return
   */
  def getElementDefinitionsOfProfile(profileUrl: String): Seq[SimpleStructureDefinition] = {
    simplifiedStructureDefinitionCache.getOrElseUpdate(profileUrl, simpleStructureDefinitionService.simplifyStructureDefinition(profileUrl))
  }

}
