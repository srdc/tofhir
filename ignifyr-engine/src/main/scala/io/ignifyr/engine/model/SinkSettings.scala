package io.ignifyr.engine.model

import io.onfhir.client.OnFhirNetworkClient
import io.onfhir.client.model.IFhirRepositorySecuritySettings
import io.onfhir.client.util.FhirClientUtil

import scala.concurrent.ExecutionContext

/**
 * Common interface for sink settings
 */
trait SinkSettings {

  /**
   * Whether the mapped resources are validated against their profile(s) before they are written; a resource with
   * validation errors is not written and is reported as an invalid resource. Needs a resource validator module
   * (Enterprise `ignifyr-validation`). Only sinks that do not validate on their own offer it: a FHIR server validates
   * what it receives, so [[FhirRepositorySinkSettings]] (and any sink that does not override this, e.g. a database
   * sink checking its rows against its tables) never asks for it.
   */
  def validateResources: Boolean = false
}

/**
 * Settings to write mapped FHIR resources to file system
 *
 * @param path                    Path to the folder or file to write the resources
 * @param contentType             Content type of the sink
 * @param numOfPartitions         Number of partitions for the file (for distributed fs)
 * @param options                 Further options (Spark data source write options)
 * @param partitionByResourceType Flag to determine whether to partition the output files by FHIR resource type.
 *                                When enabled, each resource type will be written to a separate directory.
 *                                Supported content types: {@link SinkContentTypes.NDJSON}, {@link SinkContentTypes.PARQUET},
 *                                {@link SinkContentTypes.DELTA_LAKE} and {@link SinkContentTypes.CSV}
 * @param partitioningColumns     Keeps partitioning columns for specific resource types.
 *                                Applicable only when data is partitioned by resource type (via "partitionByResourceType").
 *                                Supported content types: {@link SinkContentTypes.PARQUET} and {@link SinkContentTypes.DELTA_LAKE}
 * @param validateResources       Whether the mapped resources are validated before they are written (see
 *                                [[SinkSettings.validateResources]])
 */
case class FileSystemSinkSettings(
    path: String,
    contentType: String,
    numOfPartitions: Int = 1,
    options: Map[String, String] = Map.empty[String, String],
    partitionByResourceType: Boolean = false,
    partitioningColumns: Map[String, List[String]] = Map.empty[String, List[String]],
    override val validateResources: Boolean = false
) extends SinkSettings {

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
case class FhirRepositorySinkSettings(
    fhirRepoUrl: String,
    securitySettings: Option[IFhirRepositorySecuritySettings] = None,
    returnMinimal: Boolean = true
) extends SinkSettings
    with IdentityServiceSettings
    with TerminologyServiceSettings {

  /**
   * Create an OnFhir client
   *
   * @param executionContext Execution context the client runs its asynchronous work on
   * @return
   */
  def createOnFhirClient(implicit executionContext: ExecutionContext): OnFhirNetworkClient =
    FhirClientUtil.createOnFhirClient(fhirRepoUrl, securitySettings)
}

/**
 * List of sink content types supported by ignifyr
 */
object SinkContentTypes {
  final val NDJSON = "ndjson"
  final val CSV = "csv"
  final val PARQUET = "parquet"
  final val DELTA_LAKE = "delta"
}
