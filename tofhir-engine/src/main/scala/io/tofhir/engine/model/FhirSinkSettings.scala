package io.tofhir.engine.model

import akka.actor.ActorSystem
import io.onfhir.client.OnFhirNetworkClient
import io.onfhir.client.model.IFhirRepositorySecuritySettings
import io.onfhir.client.util.FhirClientUtil
import io.tofhir.engine.data.write.FileSystemWriter.SinkContentTypes

/**
 * Common interface for sink settings
 */
trait FhirSinkSettings

/**
 * Settings to write mapped FHIR resources to file system
 *
 * @param path                    Path to the folder or file to write the resources
 * @param contentType             Content type of the sink
 * @param numOfPartitions         Number of partitions for the file (for distributed fs)
 * @param options                 Further options (Spark data source write options)
 * @param partitionByResourceType Flag to determine whether to partition the output files by FHIR resource type.
 *                                When enabled, each resource type will be written to a separate directory.
 *                                Supported content types: {@link SinkContentTypes.NDJSON}, {@link SinkContentTypes.PARQUET}
 *                                and {@link SinkContentTypes.DELTA_LAKE}
 * @param partitioningColumns     Keeps partitioning columns for specific resource types.
 *                                Applicable only when data is partitioned by resource type (via "partitionByResourceType").
 *                                Supported content types: {@link SinkContentTypes.PARQUET} and {@link SinkContentTypes.DELTA_LAKE}
 */
case class FileSystemSinkSettings(path: String,
                                  contentType: String,
                                  numOfPartitions: Int = 1,
                                  options: Map[String, String] = Map.empty[String, String],
                                  partitionByResourceType: Boolean = false,
                                  partitioningColumns: Map[String, List[String]] = Map.empty[String, List[String]]) extends FhirSinkSettings {

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