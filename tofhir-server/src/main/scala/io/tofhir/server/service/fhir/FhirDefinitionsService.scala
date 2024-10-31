package io.tofhir.server.service.fhir

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpRequest}
import com.typesafe.scalalogging.Logger
import io.onfhir.api
import io.onfhir.api.Resource
import io.onfhir.config.{BaseFhirConfig, FSConfigReader, IFhirConfigReader}
import io.onfhir.r4.config.FhirR4Configurator
import io.onfhir.r5.config.FhirR5Configurator
import io.tofhir.common.model.Json4sSupport._
import io.tofhir.common.model.SimpleStructureDefinition
import io.tofhir.engine.Execution.actorSystem
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.util.{FileUtils, MajorFhirVersion}
import io.tofhir.server.common.model.BadRequest
import io.tofhir.server.fhir.{FhirDefinitionsConfig, FhirEndpointResourceReader}
import io.tofhir.server.model.ProfileInfo
import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.JsonMethods.compact

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

  // A cache to hold rtype -> ProfileInfo tuples which mean the profileUrl is of type rtype
  val profileInfoCache: mutable.Map[String, Set[ProfileInfo]] = mutable.HashMap()

  // TODO: Add comment to explain what this cache does
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
  def getProfilesFor(rtype: String): Set[ProfileInfo] = {
    val profileInfoSet = baseFhirConfig.profileRestrictions
      .filter(urlMap => // (url -> (version -> ProfileRestrictions)) Filter the ones whose ProfileRestrictions contain the rtype
        urlMap._2.exists(versionMap => versionMap._2.resourceType == rtype)) // The resourceType must be the same for all ProfileRestrictions of a URL (the StructureDefinition). .exists and .forall should evaluate to the same thing
      .flatMap(urlMap => urlMap._2.keySet.map(version => ProfileInfo(urlMap._1, version))) // Create the Set of ProfileInfo for each version of each url (and flatten with flatMap)
      .toSet
    profileInfoCache.getOrElseUpdate(rtype, profileInfoSet)
  }

  /**
   * Retrieves the simplified structure definitions for a given FHIR profile or schema URL.
   *
   * This method first attempts to obtain the structure definition from the SimpleStructureDefinitionService.
   * If the profile is not found, it queries the Schema Repository.
   *
   * @param url The URL of the FHIR profile or Schema to retrieve the structure definition for.
   * @return A sequence of SimpleStructureDefinition objects representing the simplified structure definition of the profile or schema.
   */
  def getElementDefinitionsOfProfile(url: String): Seq[SimpleStructureDefinition] = {
    // parse the canonical URL to extract URL and optional version part
    val canonicalParts = url.split('|')
    val urlWithoutVersion = canonicalParts.head
    val version = canonicalParts.drop(1).lastOption
    // retrieve the simplified structure definitions from the cache, or simplify it using SimpleStructureDefinitionService if not cached
    simplifiedStructureDefinitionCache.getOrElseUpdate(url, simpleStructureDefinitionService.simplifyStructureDefinition(urlWithoutVersion, version))
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
