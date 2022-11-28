package io.tofhir.server.fhir

import io.onfhir.api.Resource
import io.onfhir.client.OnFhirNetworkClient
import io.onfhir.config.{FSConfigReader, IFhirConfigReader}
import io.tofhir.engine.model.{BasicAuthenticationSettings, BearerTokenAuthorizationSettings}
import io.tofhir.engine.util.FhirClientUtil

/**
 * FHIR configuration reader from FHIR endpoint
 */
class FhirEndpointResourceReader(fhirDefinitionsConfig: FhirDefinitionsConfig) extends IFhirConfigReader {

  // So that we can read the base definitions from the standard's bundle file which is available in onfhir-server-r4
  val fsConfigReader: IFhirConfigReader = new FSConfigReader()

  val fhirClient: OnFhirNetworkClient = createOnFhirClient

  private def createOnFhirClient: OnFhirNetworkClient = {
    if(fhirDefinitionsConfig.definitionsFHIREndpoint.isEmpty) {
      throw new IllegalStateException("FhirEndpointResourceReader can only be instantiated with a valid FHIR endpoint in fhir.definitions-fhir-endpoint")
    }

    import io.tofhir.engine.Execution.actorSystem
    fhirDefinitionsConfig.definitionsFHIRAuthMethod match {
      case None => FhirClientUtil.createOnFhirClient(fhirDefinitionsConfig.definitionsFHIREndpoint.get)
      case Some(method) => FhirAuthMethod.withName(method) match {
        case FhirAuthMethod.BASIC =>
          if(fhirDefinitionsConfig.authBasicUsername.isEmpty || fhirDefinitionsConfig.authBasicPassword.isEmpty) {
            throw new IllegalArgumentException("For basic authentication, a username and password must be provided!")
          }
          FhirClientUtil.createOnFhirClient(fhirDefinitionsConfig.definitionsFHIREndpoint.get,
            Some(BasicAuthenticationSettings(fhirDefinitionsConfig.authBasicUsername.get, fhirDefinitionsConfig.authBasicPassword.get)))
        case FhirAuthMethod.BEARER_TOKEN =>
          if(fhirDefinitionsConfig.authTokenClientId.isEmpty || fhirDefinitionsConfig.authTokenClientSecret.isEmpty || fhirDefinitionsConfig.authTokenScopeList.isEmpty || fhirDefinitionsConfig.authTokenEndpoint.isEmpty) {
            throw new IllegalArgumentException("For bearer token authentication; client-id, client-secret, token-endpoint and scopes (event if empty) must be provided!")
          }
          FhirClientUtil.createOnFhirClient(fhirDefinitionsConfig.definitionsFHIREndpoint.get,
            Some(BearerTokenAuthorizationSettings(fhirDefinitionsConfig.authTokenClientId.get, fhirDefinitionsConfig.authTokenClientSecret.get, fhirDefinitionsConfig.authTokenScopeList.get, fhirDefinitionsConfig.authTokenEndpoint.get)))
      }
    }
    FhirClientUtil.createOnFhirClient(fhirDefinitionsConfig.definitionsFHIREndpoint.get, None) // FIXME
  }

  override def readStandardBundleFile(fileName: String, resourceTypeFilter: Set[String]): Seq[Resource] = fsConfigReader.readStandardBundleFile(fileName, resourceTypeFilter)

  override def getInfrastructureResources(rtype: String): Seq[Resource] = {
//    val pathsToSearch =
//      rtype match {
//        case FHIR_FOUNDATION_RESOURCES.FHIR_STRUCTURE_DEFINITION =>
//          profilesPath -> DEFAULT_RESOURCE_PATHS.PROFILES_FOLDER
//        case FHIR_FOUNDATION_RESOURCES.FHIR_VALUE_SET =>
//          valueSetsPath -> DEFAULT_RESOURCE_PATHS.VALUESETS_PATH
//        case FHIR_FOUNDATION_RESOURCES.FHIR_CODE_SYSTEM =>
//          codeSystemsPath -> DEFAULT_RESOURCE_PATHS.CODESYSTEMS_PATH
//        case FHIR_FOUNDATION_RESOURCES.FHIR_OPERATION_DEFINITION | FHIR_FOUNDATION_RESOURCES.FHIR_COMPARTMENT_DEFINITION | FHIR_FOUNDATION_RESOURCES.FHIR_SEARCH_PARAMETER =>
//          Seq.empty[Resource]
//      }

    val a = fhirDefinitionsConfig.definitionsRootURLs match {
      case None => fhirClient.search(rtype)
      case Some(urls) => fhirClient.search(rtype).where("url:below", urls.mkString(","))
    }
    a.execute()

    null

  }

  override def readCapabilityStatement(): Resource = ???
}

