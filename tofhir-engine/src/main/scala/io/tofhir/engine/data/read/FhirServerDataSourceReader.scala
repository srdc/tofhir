package io.tofhir.engine.data.read

import io.onfhir.spark.reader.FhirApiReader.OPTIONS
import io.tofhir.engine.model.{BasicAuthenticationSettings, BearerTokenAuthorizationSettings, FhirServerSource, FhirServerSourceSettings, FixedTokenAuthenticationSettings}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

/**
 * Reader class for retrieving data from a FHIR server using a custom Spark data source.
 *
 * @param spark The SparkSession
 */
class FhirServerDataSourceReader(spark: SparkSession) extends BaseDataSourceReader[FhirServerSource, FhirServerSourceSettings] {

  /**
   * No validation required for this data source since the data comes form a Fhir Server indicating that it is valid
   * */
  override val needCardinalityValidation: Boolean = false

  /**
   * Reads data from the specified FHIR server source.
   *
   * @param mappingSource  Context/configuration information for the FHIR server source.
   * @param sourceSettings Settings for the FHIR server source, including column conversion information.
   * @param schema         Optional schema for the source data.
   * @param timeRange      Optional time range to filter the data.
   * @param limit          Optional limit on the number of rows to read.
   * @param jobId          Optional identifier of the mapping job executing the read operation.
   * @return A DataFrame containing the source data from the FHIR server.
   * @throws IllegalArgumentException If the path is not a directory for streaming jobs.
   * @throws NotImplementedError      If the specified source format is not implemented.
   */
  override def read(mappingSource: FhirServerSource, sourceSettings: FhirServerSourceSettings, schema: Option[StructType], timeRange: Option[(LocalDateTime, LocalDateTime)], limit: Option[Int] = Option.empty, jobId: Option[String] = Option.empty): DataFrame = {
    // extract Spark option for the authentication from the given source settings
    val authenticationOptions = extractAuthenticationOptions(sourceSettings)
    // read data from a FHIR Server using a custom Spark data source i.e. io.onfhir.spark.reader.FhirApiTableProvider
    spark
      .read
      .format("io.onfhir.spark.reader.FhirApiTableProvider")
      .option("url", sourceSettings.serverUrl)
      .option("rtype", mappingSource.resourceType)
      .option("query", s"?${mappingSource.query.getOrElse("")}")
      .options(authenticationOptions)
      .load()
  }

  /**
   * Extracts authentication options from the provided FhirServerSourceSettings.
   *
   * @param sourceSettings The FhirServerSourceSettings containing security settings.
   * @return A map of authentication options extracted from the source settings.
   */
  private def extractAuthenticationOptions(sourceSettings: FhirServerSourceSettings): Map[String, String] = {
    sourceSettings.securitySettings
      .map {
        case BearerTokenAuthorizationSettings(clientId, clientSecret, requiredScopes, authzServerTokenEndpoint, clientAuthenticationMethod) =>
          Map(
            OPTIONS.AUTH -> OPTIONS.AUTH_TYPE.BEARER_TOKEN,
            OPTIONS.AUTH_BEARER_TOKEN_PARAMETERS.CLIENT_ID -> clientId,
            OPTIONS.AUTH_BEARER_TOKEN_PARAMETERS.CLIENT_SECRET -> clientSecret,
            OPTIONS.AUTH_BEARER_TOKEN_PARAMETERS.REQUIRED_SCOPES -> requiredScopes.mkString(","),
            OPTIONS.AUTH_BEARER_TOKEN_PARAMETERS.AUTHZ_SERVER_TOKEN_ENDPOINT -> authzServerTokenEndpoint,
            OPTIONS.AUTH_BEARER_TOKEN_PARAMETERS.CLIENT_AUTHENTICATION_METHOD -> clientAuthenticationMethod
          )
        case BasicAuthenticationSettings(username, password) =>
          Map(
            OPTIONS.AUTH -> OPTIONS.AUTH_TYPE.BASIC,
            OPTIONS.AUTH_BASIC_PARAMETERS.USERNAME -> username,
            OPTIONS.AUTH_BASIC_PARAMETERS.PASSWORD -> password
          )
        case FixedTokenAuthenticationSettings(token) =>
          Map(
            OPTIONS.AUTH -> OPTIONS.AUTH_TYPE.FIXED_TOKEN,
            OPTIONS.AUTH_FIXED_TOKEN_PARAMETERS.TOKEN -> token
          )
      }.getOrElse(Map.empty)
  }
}
