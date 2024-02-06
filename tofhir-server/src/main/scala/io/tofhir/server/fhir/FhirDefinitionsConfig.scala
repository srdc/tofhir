package io.tofhir.server.fhir

import com.typesafe.config.Config

import scala.jdk.CollectionConverters._
import scala.util.Try

class FhirDefinitionsConfig(fhirDefinitionsConfig: Config) {

  /** Major FHIR version (R4 or R5) */
  lazy val majorFhirVersion: String = Try(fhirDefinitionsConfig.getString("fhir-version")).getOrElse("R5")
  /**
   * List of root URLs while retrieving the definitions (profiles, valuesets, codesystems).
   * The definitions below the given root URLs will be retrieved from the configured paths or FHIR endpoints.
   */
  lazy val definitionsRootURLs: Option[Seq[String]] = Try(fhirDefinitionsConfig.getStringList("definitions-root-urls").asScala.toSeq).toOption

  /** FHIR URL to retrieve resource definitions (profiles, valuesets and codesystems). */
  lazy val definitionsFHIREndpoint: Option[String] = Try(fhirDefinitionsConfig.getString("definitions-fhir-endpoint")).toOption

  /** Auth configurations for the definitions FHIR endpoint */
  lazy val definitionsFHIRAuthMethod: Option[String] = Try(fhirDefinitionsConfig.getString("fhir-endpoint-auth.method")).toOption
  lazy val authBasicUsername: Option[String] = Try(fhirDefinitionsConfig.getString("fhir-endpoint-auth.basic.username")).toOption
  lazy val authBasicPassword: Option[String] = Try(fhirDefinitionsConfig.getString("fhir-endpoint-auth.basic.password")).toOption
  lazy val authTokenClientId: Option[String] = Try(fhirDefinitionsConfig.getString("fhir-endpoint-auth.token.client-id")).toOption
  lazy val authTokenClientSecret: Option[String] = Try(fhirDefinitionsConfig.getString("fhir-endpoint-auth.token.client-secret")).toOption
  lazy val authTokenScopeList: Option[Seq[String]] = Try(fhirDefinitionsConfig.getStringList("fhir-endpoint-auth.token.scopes").asScala.toSeq).toOption
  lazy val authTokenEndpoint: Option[String] = Try(fhirDefinitionsConfig.getString("fhir-endpoint-auth.token.token-endpoint")).toOption
  lazy val authFixedToken: Option[String] = Try(fhirDefinitionsConfig.getString("fhir-endpoint-auth.fixed-token")).toOption
  /** Path to the zip file or folder that includes the FHIR resource and data type profile definitions (FHIR StructureDefinition) to be served by toFHIR webserver so that mappings can be performed accordingly. */
  lazy val profilesPath: Option[String] = Try(fhirDefinitionsConfig.getString("profiles-path")).toOption

  /** Path to the zip file or folder that includes the FHIR Value Set definitions (FHIR ValueSet) that are referenced by your FHIR profiles. */
  lazy val valuesetsPath: Option[String] = Try(fhirDefinitionsConfig.getString("valuesets-path")).toOption

  /** Path to the zip file or folder that includes the FHIR Code system definitions (FHIR CodeSystem) that are referenced by your FHIR value sets. */
  lazy val codesystemsPath: Option[String] = Try(fhirDefinitionsConfig.getString("codesystems-path")).toOption
}
