package io.tofhir.server.service

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.RawHeader
import com.typesafe.scalalogging.Logger
import io.onfhir.api
import io.onfhir.api.Resource
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpRequest}
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.JsonMethods.compact
import io.onfhir.config.{BaseFhirConfig, FSConfigReader, IFhirConfigReader}
import io.onfhir.r4.config.FhirR4Configurator
import io.onfhir.r5.config.FhirR5Configurator
import io.tofhir.engine.util.{FileUtils, MajorFhirVersion}
import io.tofhir.server.fhir.{FhirDefinitionsConfig, FhirEndpointResourceReader}
import io.tofhir.common.model.SimpleStructureDefinition
import org.json4s.JsonAST.JObject
import io.tofhir.server.model.Json4sSupport._
import io.tofhir.engine.Execution.actorSystem
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.server.common.model.BadRequest

import java.net.{MalformedURLException, URL}
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, FiniteDuration}

class FhirDefinitionsService(fhirDefinitionsConfig: FhirDefinitionsConfig) {

  private val logger: Logger = Logger(this.getClass)
  // timeout for the proxy request to the FHIR validator
  val timeout: FiniteDuration = 20.seconds

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

      new FSConfigReader(fhirVersion = fhirDefinitionsConfig.majorFhirVersion,
                         profilesPath = profilesPath,
                         codeSystemsPath = codeSystemsPath,
                         valueSetsPath = valueSetsPath)
  }


  val baseFhirConfig: BaseFhirConfig = fhirDefinitionsConfig.majorFhirVersion match {
    case MajorFhirVersion.R4 =>
      new FhirR4Configurator().initializePlatform(fhirConfigReader)
    case MajorFhirVersion.R5 =>
      new FhirR5Configurator().initializePlatform(fhirConfigReader)
    case _ =>
      throw new RuntimeException("Unsupported FHIR version.")
  }

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

  /**
   * Validates a FHIR resource against a specified FHIR validation URL.
   *
   * @param requestBody       The FHIR resource to be validated.
   * @param fhirValidationUrl The URL for FHIR validation.
   * @return A Future containing the validated FHIR resource as a JObject.
   * @throws BadRequestException If the provided FHIR validation URL is not a valid URL.
   */
  def validateResource(requestBody: Resource, fhirValidationUrl: String): Future[JObject] = {
    if (!isValidUrl(fhirValidationUrl)) {
      throw BadRequest("Invalid fhirValidationUrl", s"$fhirValidationUrl is not a valid URL.")
    }
    val proxiedRequest = HttpRequest(
      method = HttpMethods.POST,
      uri = s"$fhirValidationUrl",
      headers = RawHeader("Content-Type", "application/json") :: Nil,
      entity = HttpEntity(ContentTypes.`application/json`, compact(JsonMethods.render(requestBody)))
    )

    // add timeout and transform response to json
    Http().singleRequest(proxiedRequest)
      .flatMap { resp => resp.entity.toStrict(timeout) }
      .map(strictEntity => {
        val response = strictEntity.data.utf8String
        JsonMethods.parse(response).extract[JObject]
      })
  }

  /**
   * Validates if the given string is a valid URL
   *
   * @param url string to validate
   * @return
   */
  private def isValidUrl(url: String): Boolean = {
    try {
      new URL(url)
      true
    } catch {
      case _: MalformedURLException => false
    }
  }

}
