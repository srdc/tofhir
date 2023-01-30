package io.tofhir.engine.config

import scala.util.Try
import com.typesafe.config.Config

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.jdk.DurationConverters._

class ToFhirEngineConfig(toFhirEngineConfig: Config) {
  /** A path to a context file/directory from where any kind of file system reading should start. */
  lazy val mappingJobFileContextPath: String = Try(toFhirEngineConfig.getString("context-path")).getOrElse(".")

  /** Path to the folder where the mappings are kept. */
  lazy val mappingRepositoryFolderPath: String = Try(toFhirEngineConfig.getString("mappings.repository.folder-path")).getOrElse("mappings")

  /** Path to the folder where the schema definitions are kept. */
  lazy val schemaRepositoryFolderPath: String = Try(toFhirEngineConfig.getString("mappings.schemas.repository.folder-path")).getOrElse("schemas")

  /** Timeout for a single mapping */
  lazy val mappingTimeout: Duration = Try(toFhirEngineConfig.getDuration("mappings.timeout").toScala).toOption.getOrElse(Duration.apply(5, TimeUnit.SECONDS))

  /** Absolute file path to the MappingJobs file while initiating the Data Integration Suite */
  lazy val initialMappingJobFilePath: Option[String] = Try(toFhirEngineConfig.getString("mapping-jobs.initial-job-file-path")).toOption

  /**
   * Number of partitions for to repartition the source data before executing the mappings for mapping jobs
   */
  lazy val partitionsForMappingJobs: Option[Int] = Try(toFhirEngineConfig.getInt("mapping-jobs.numOfPartitions")).toOption

  /**
   * Max batch size to execute for batch executions, if number of records exceed this the source data will be divided into batches
   */
  lazy val maxBatchSizeForMappingJobs: Option[Long] = Try(toFhirEngineConfig.getLong("mapping-jobs.maxBatchSize")).toOption

  /** The # of FHIR resources in the group while executing (create/update) a batch operation. */
  lazy val fhirWriterBatchGroupSize: Int = Try(toFhirEngineConfig.getInt("fhir-server-writer.batch-group-size")).getOrElse(10)

  /** Path to the folder where the execution times of scheduled mapping jobs are kept. */
  lazy val toFhirDb: Option[String] = Try(toFhirEngineConfig.getString("db")).toOption
}