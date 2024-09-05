package io.tofhir.engine.model

import akka.actor.ActorSystem
import io.onfhir.client.OnFhirNetworkClient
import io.tofhir.engine.data.write.FileSystemWriter.SinkFileFormats
import io.tofhir.engine.util.FhirClientUtil

/**
 * Common interface for sink settings
 */
trait FhirSinkSettings

/**
 * Settings to write mapped FHIR resources to file system
 *
 * @param path                    Path to the folder or file to write the resources
 * @param fileFormat              File format if not inferred from the path
 * @param numOfPartitions         Number of partitions for the file (for distributed fs)
 * @param options                 Further options (Spark data source write options)
 * @param partitionByResourceType Flag to determine whether to partition the output files by FHIR resource type.
 *                                When enabled, each resource type will be written to a separate directory.
 *                                Supported file formats: {@link SinkFileFormats.NDJSON}, {@link SinkFileFormats.PARQUET}
 *                                and {@link SinkFileFormats.DELTA_LAKE}
 * @param partitioningColumns     Keeps partitioning columns for specific resource types.
 *                                Applicable only when data is partitioned by resource type (via "partitionByResourceType").
 *                                Supported file formats: {@link SinkFileFormats.PARQUET} and {@link SinkFileFormats.DELTA_LAKE}
 */
case class FileSystemSinkSettings(path: String,
                                  fileFormat: Option[String] = None,
                                  numOfPartitions: Int = 1,
                                  options: Map[String, String] = Map.empty[String, String],
                                  partitionByResourceType: Boolean = false,
                                  partitioningColumns: Map[String, List[String]] = Map.empty[String, List[String]]) extends FhirSinkSettings {
  /**
   * Determines the file format to use, inferred from the path if not explicitly provided.
   *
   * @return The file format as a string, either inferred from the file extension or provided explicitly.
   */
  def sinkType: String = fileFormat.getOrElse(path.split('.').last)

  /**
   * Retrieves the partition columns for a given resource type.
   *
   * @param resourceType The FHIR resource type for which to retrieve the partitioning columns.
   * @return A list of columns to partition by, or an empty list if no partitioning is defined for the resource type.
   */
  def getPartitioningColumns(resourceType: String): List[String] = {
    partitioningColumns.getOrElse(resourceType, List.empty[String])
  }
}

/**
 * Settings for a FHIR repository to store the mapped resources
 *
 * @param fhirRepoUrl      FHIR endpoint root url
 * @param securitySettings Security settings if target API is secured
 * @param returnMinimal    Whether 'return=minimal' header should be added to the FHIR batch request while writing the
 *                         resources into the FHIR Repository. If this header is added, the response does not return the
 *                         body which improves the performance.
 */
case class FhirRepositorySinkSettings(fhirRepoUrl: String,
                                      securitySettings: Option[IFhirRepositorySecuritySettings] = None,
                                      returnMinimal: Boolean = true) extends FhirSinkSettings with IdentityServiceSettings with TerminologyServiceSettings {
  /**
   * Create an OnFhir client
   *
   * @param actorSystem
   * @return
   */
  def createOnFhirClient(implicit actorSystem: ActorSystem): OnFhirNetworkClient = FhirClientUtil.createOnFhirClient(fhirRepoUrl, securitySettings)
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
 *
 * @param username Username for basic authentication
 * @param password Password for basic authentication
 */
case class BasicAuthenticationSettings(username: String, password: String) extends IFhirRepositorySecuritySettings

/**
 * Security settings for FHIR API access via fixed token
 *
 * @param token The fixed token
 */
case class FixedTokenAuthenticationSettings(token: String) extends IFhirRepositorySecuritySettings