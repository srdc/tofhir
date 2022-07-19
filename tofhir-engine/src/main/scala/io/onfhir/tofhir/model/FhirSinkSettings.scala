package io.onfhir.tofhir.model

import io.onfhir.tofhir.config.MappingErrorHandling.MappingErrorHandling

/**
 * Common interface for sink settings
 */
trait FhirSinkSettings

/**
 * Settings for a FHIR repository to store the mapped resources
 *
 * @param fhirRepoUrl      FHIR endpoint root url
 * @param securitySettings Security settings if target API is secured
 */
case class FhirRepositorySinkSettings(fhirRepoUrl: String, securitySettings: Option[FhirRepositorySecuritySettings] = None,
                                      writeErrorHandling: MappingErrorHandling) extends FhirSinkSettings

/**
 * Security settings for FHIR API access
 *
 * @param clientId                   OpenID Client identifier assigned to toFhir
 * @param clientSecret               OpenID Client secret given to toFhir
 * @param requiredScopes             List of required scores to write the resources
 * @param authzServerTokenEndpoint   Authorization servers token endpoint
 * @param clientAuthenticationMethod Client authentication method
 */
case class FhirRepositorySecuritySettings(clientId: String,
                                          clientSecret: String,
                                          requiredScopes: Seq[String],
                                          authzServerTokenEndpoint: String,
                                          clientAuthenticationMethod: String = "client_secret_basic")

