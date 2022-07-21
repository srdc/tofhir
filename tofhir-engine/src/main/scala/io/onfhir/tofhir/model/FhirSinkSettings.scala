package io.onfhir.tofhir.model

import akka.actor.ActorSystem
import io.onfhir.client.OnFhirNetworkClient
import io.onfhir.tofhir.config.MappingErrorHandling.MappingErrorHandling

/**
 * Common interface for sink settings
 */
trait FhirSinkSettings

/**
 * Settings for a FHIR repository to store the mapped resources
 *
 * @param fhirRepoUrl         FHIR endpoint root url
 * @param securitySettings    Security settings if target API is secured
 * @param writeErrorHandling  How to handle error while writing mapped FHIR resources to this FHIR repository
 */
case class FhirRepositorySinkSettings(fhirRepoUrl: String,
                                      securitySettings: Option[IFhirRepositorySecuritySettings] = None,
                                      writeErrorHandling: MappingErrorHandling) extends FhirSinkSettings with IdentityServiceSettings with TerminologyServiceSettings {
  /**
   * Create an OnFhir client
   * @param actorSystem
   * @return
   */
  def createOnFhirClient(implicit actorSystem: ActorSystem):OnFhirNetworkClient = {
    val client = OnFhirNetworkClient.apply(fhirRepoUrl)
    securitySettings
      .map {
        case BearerTokenAuthorizationSettings(clientId, clientSecret, requiredScopes, authzServerTokenEndpoint, clientAuthenticationMethod) =>
          client.withOpenIdBearerTokenAuthentication(clientId, clientSecret, requiredScopes,authzServerTokenEndpoint, clientAuthenticationMethod)
        case BasicAuthenticationSettings(username, password) => client.withBasicAuthentication(username, password)
      }
      .getOrElse(client)
  }
}

/**
 * Interface for security settings
 */
trait IFhirRepositorySecuritySettings

/**
 * Security settings for FHIR API access via bearer token
 *
 * @param clientId                   OpenID Client identifier assigned to toFhir
 * @param clientSecret               OpenID Client secret given to toFhir
 * @param requiredScopes             List of required scores to write the resources
 * @param authzServerTokenEndpoint   Authorization servers token endpoint
 * @param clientAuthenticationMethod Client authentication method
 */
case class BearerTokenAuthorizationSettings(clientId: String,
                                          clientSecret: String,
                                          requiredScopes: Seq[String],
                                          authzServerTokenEndpoint: String,
                                          clientAuthenticationMethod: String = "client_secret_basic") extends IFhirRepositorySecuritySettings

/**
 * Security settings for FHIR API access via basic authentication
 * @param username  Username for basic authentication
 * @param password  Password for basic authentication
 */
case class BasicAuthenticationSettings(username:String, password:String) extends IFhirRepositorySecuritySettings
