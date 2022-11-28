package io.tofhir.engine.util

import akka.actor.ActorSystem
import io.onfhir.client.OnFhirNetworkClient
import io.tofhir.engine.model.{BasicAuthenticationSettings, BearerTokenAuthorizationSettings, IFhirRepositorySecuritySettings}

object FhirClientUtil {
  /**
   * Create an OnFhir client
   *
   * @param actorSystem
   * @return
   */
  def createOnFhirClient(fhirRepoUrl: String, securitySettings: Option[IFhirRepositorySecuritySettings] = None)(implicit actorSystem: ActorSystem): OnFhirNetworkClient = {
    val client = OnFhirNetworkClient.apply(fhirRepoUrl)
    securitySettings
      .map {
        case BearerTokenAuthorizationSettings(clientId, clientSecret, requiredScopes, authzServerTokenEndpoint, clientAuthenticationMethod) =>
          client.withOpenIdBearerTokenAuthentication(clientId, clientSecret, requiredScopes, authzServerTokenEndpoint, clientAuthenticationMethod)
        case BasicAuthenticationSettings(username, password) => client.withBasicAuthentication(username, password)
      }
      .getOrElse(client)
  }
}
