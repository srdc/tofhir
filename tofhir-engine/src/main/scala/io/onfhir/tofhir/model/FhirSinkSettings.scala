package io.onfhir.tofhir.model

import akka.actor.ActorSystem
import io.onfhir.client.OnFhirNetworkClient
import io.onfhir.tofhir.config.ErrorHandlingType.ErrorHandlingType

/**
 * Common interface for sink settings
 */
trait FhirSinkSettings

/**
 * Settings to write mapped FHIR resources to file system
 * @param path                      Path to the folder or file to write the resources
 * @param fileFormat                File format if not inferred from the path
 * @param numOfPartitions           Number of partitions for the file (for distributed fs)
 * @param options                   Further options (Spark data source write options)
 */
case class FileSystemSinkSettings(path:String,
                                  fileFormat:Option[String] = None,
                                  numOfPartitions:Int = 1,
                                  options:Map[String,String] = Map.empty[String, String]) extends FhirSinkSettings {
  def sinkType:String = fileFormat.getOrElse(path.split('.').last)

}

/**
 * Settings for a FHIR repository to store the mapped resources
 *
 * @param fhirRepoUrl         FHIR endpoint root url
 * @param securitySettings    Security settings if target API is secured
 * @param errorHandling  How to handle error while writing mapped FHIR resources to this FHIR repository
 */
case class FhirRepositorySinkSettings(fhirRepoUrl: String,
                                      securitySettings: Option[IFhirRepositorySecuritySettings] = None,
                                      errorHandling: Option[ErrorHandlingType] = None) extends FhirSinkSettings with IdentityServiceSettings with TerminologyServiceSettings {
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
