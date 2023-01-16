package io.tofhir.server.fhir

import io.onfhir.api.{FHIR_FOUNDATION_RESOURCES, Resource}
import io.onfhir.client.OnFhirNetworkClient
import io.onfhir.config.{FSConfigReader, IFhirConfigReader}
import io.tofhir.engine.model.{BasicAuthenticationSettings, BearerTokenAuthorizationSettings}
import io.tofhir.engine.util.FhirClientUtil
import io.tofhir.engine.Execution.actorSystem
import actorSystem.dispatcher

import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

/**
 * FHIR configuration reader from FHIR endpoint
 */
class FhirEndpointResourceReader(fhirDefinitionsConfig: FhirDefinitionsConfig) extends IFhirConfigReader {

  // So that we can read the base definitions from the standard's bundle file which is available in onfhir-server-r4
  val fsConfigReader: IFhirConfigReader = new FSConfigReader()

  val fhirClient: OnFhirNetworkClient = createOnFhirClient

  private def createOnFhirClient: OnFhirNetworkClient = {
    if (fhirDefinitionsConfig.definitionsFHIREndpoint.isEmpty) {
      throw new IllegalStateException("FhirEndpointResourceReader can only be instantiated with a valid FHIR endpoint in fhir.definitions-fhir-endpoint")
    }

    fhirDefinitionsConfig.definitionsFHIRAuthMethod match {
      case None => FhirClientUtil.createOnFhirClient(fhirDefinitionsConfig.definitionsFHIREndpoint.get)
      case Some(method) => FhirAuthMethod.withName(method) match {
        case FhirAuthMethod.BASIC =>
          if (fhirDefinitionsConfig.authBasicUsername.isEmpty || fhirDefinitionsConfig.authBasicPassword.isEmpty) {
            throw new IllegalArgumentException("For basic authentication, a username and password must be provided!")
          }
          FhirClientUtil.createOnFhirClient(fhirDefinitionsConfig.definitionsFHIREndpoint.get,
            Some(BasicAuthenticationSettings(fhirDefinitionsConfig.authBasicUsername.get, fhirDefinitionsConfig.authBasicPassword.get)))
        case FhirAuthMethod.BEARER_TOKEN =>
          if (fhirDefinitionsConfig.authTokenClientId.isEmpty || fhirDefinitionsConfig.authTokenClientSecret.isEmpty || fhirDefinitionsConfig.authTokenScopeList.isEmpty || fhirDefinitionsConfig.authTokenEndpoint.isEmpty) {
            throw new IllegalArgumentException("For bearer token authentication; client-id, client-secret, token-endpoint and scopes (event if empty) must be provided!")
          }
          FhirClientUtil.createOnFhirClient(fhirDefinitionsConfig.definitionsFHIREndpoint.get,
            Some(BearerTokenAuthorizationSettings(fhirDefinitionsConfig.authTokenClientId.get, fhirDefinitionsConfig.authTokenClientSecret.get, fhirDefinitionsConfig.authTokenScopeList.get, fhirDefinitionsConfig.authTokenEndpoint.get)))
      }
    }
  }

  override def readStandardBundleFile(fileName: String, resourceTypeFilter: Set[String]): Seq[Resource] = fsConfigReader.readStandardBundleFile(fileName, resourceTypeFilter)

  override def getInfrastructureResources(rtype: String): Seq[Resource] = {
    val fhirSearchRequest = fhirDefinitionsConfig.definitionsRootURLs match {
      case Some(urls) if urls.nonEmpty =>
        fhirClient.search(rtype).where("url:below", urls.map(url => if (url.endsWith("/")) url.dropRight(1) else url).mkString(","))
      case _ =>
        throw new IllegalArgumentException("fhir.definitions-root-urls must be defined so that the system can read resources under those urls!!")
    }

    rtype match {
      case FHIR_FOUNDATION_RESOURCES.FHIR_SEARCH_PARAMETER | FHIR_FOUNDATION_RESOURCES.FHIR_OPERATION_DEFINITION | FHIR_FOUNDATION_RESOURCES.FHIR_COMPARTMENT_DEFINITION =>
        Seq.empty[Resource] // SearchParameter, OperationDefinition and CompartmentDefinition are not supported!
      case FHIR_FOUNDATION_RESOURCES.FHIR_STRUCTURE_DEFINITION | FHIR_FOUNDATION_RESOURCES.FHIR_VALUE_SET | FHIR_FOUNDATION_RESOURCES.FHIR_CODE_SYSTEM =>
        Await.result(
          fhirSearchRequest.executeAndMergeBundle().map(_.searchResults).transform {
            case Success(value) => Try(value)
            case Failure(exception) =>
              actorSystem.log.error(s"An error occurred while searching infrastructure resources from ${fhirSearchRequest.request.requestUri}", exception)
              throw exception
          },
          FiniteDuration(30, TimeUnit.SECONDS))
    }

  }

  override def readCapabilityStatement(): Resource = {
    Await.result(
      fhirClient.search("metadata").executeAndReturnResource().transform {
        case Success(value) => Try(value)
        case Failure(exception) =>
          actorSystem.log.error(s"An error occurred while reading the CapabilityStatement", exception)
          throw exception
      },
      FiniteDuration(5, TimeUnit.SECONDS))
  }
}

