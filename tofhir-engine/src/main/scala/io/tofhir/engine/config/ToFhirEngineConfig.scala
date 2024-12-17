package io.tofhir.engine.config

import scala.util.Try
import com.typesafe.config.Config
import io.tofhir.engine.util.FileUtils

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.jdk.DurationConverters._

class ToFhirEngineConfig(toFhirEngineConfig: Config) {
  /** A path to a context file/directory from where any kind of file system reading should start. */
  lazy val contextPath: String = Try(toFhirEngineConfig.getString("context-path")).getOrElse(".")

  /** Path to the folder where the mappings are kept. */
  lazy val mappingRepositoryFolderPath: String = Try(toFhirEngineConfig.getString("mappings.repository.folder-path")).getOrElse("mappings")

  /** Path to the folder where the mapping context files are kept. */
  lazy val mappingContextRepositoryFolderPath: String = Try(toFhirEngineConfig.getString("mappings.contexts.repository.folder-path")).getOrElse("mapping-contexts")

  /** Path to the folder where the schema definitions are kept. */
  lazy val schemaRepositoryFolderPath: String = Try(toFhirEngineConfig.getString("mappings.schemas.repository.folder-path")).getOrElse("schemas")

  /** Specific FHIR version for schemas in the schema repository. Represents fhirVersion field in the standard StructureDefinition */
  lazy val schemaRepositoryFhirVersion: String = Try(toFhirEngineConfig.getString("mappings.schemas.fhir-version")).getOrElse("4.0.1")

  /** Path to the folder where the job definitions are kept. */
  lazy val jobRepositoryFolderPath: String = Try(toFhirEngineConfig.getString("mapping-jobs.repository.folder-path")).getOrElse("mapping-jobs")

  /** Path to the folder where the terminology system definitions are kept. */
  lazy val terminologySystemFolderPath: String = Try(toFhirEngineConfig.getString("terminology-systems.folder-path")).getOrElse("terminology-systems")

  /** Timeout for a single mapping */
  lazy val mappingTimeout: Duration = Try(toFhirEngineConfig.getDuration("mappings.timeout").toScala).toOption.getOrElse(Duration.apply(5, TimeUnit.SECONDS))

  /** Absolute file path to the MappingJobs file while initiating the Data Integration Suite */
  lazy val initialMappingJobFilePath: Option[String] = Try(toFhirEngineConfig.getString("mapping-jobs.initial-job-file-path")).toOption

  /**
   * Number of partitions for to repartition the source data before executing the mappings for mapping jobs
   */
  lazy val partitionsForMappingJobs: Option[Int] = Try(toFhirEngineConfig.getInt("mapping-jobs.numOfPartitions")).toOption

  /**
   * Max chunk size to execute for batch executions, if number of records exceed this, the source data will be divided into chunks
   */
  lazy val maxChunkSizeForMappingJobs: Option[Long] = Try(toFhirEngineConfig.getLong("mapping-jobs.maxChunkSize")).toOption

  /** The # of FHIR resources in the group while executing (create/update) a FHIR batch operation. */
  lazy val fhirWriterBatchGroupSize: Int = Try(toFhirEngineConfig.getInt("fhir-server-writer.batch-group-size")).getOrElse(10)

  /** Path to the folder which acts as the folder database of toFHIR*/
  lazy val toFhirDbFolderPath: String = Try(toFhirEngineConfig.getString("db-path")).getOrElse("tofhir-db")

  /** The parent-folder where the data sources of errors received while running mapping are stored. */
  lazy val erroneousRecordsFolder: String = FileUtils.getPath(Try(toFhirEngineConfig.getString("archiving.erroneous-records-folder")).getOrElse("erroneous-records-folder")).toString

  /** Folder path where the archive of the processed source data is stored. */
  lazy val archiveFolder: String = FileUtils.getPath(Try(toFhirEngineConfig.getString("archiving.archive-folder")).getOrElse("archive-folder")).toString

  /** Period (in milliseconds) to run archiving task for file streaming jobs */
  lazy val streamArchivingFrequency: Int = Try(toFhirEngineConfig.getInt("archiving.stream-archiving-frequency")).toOption.getOrElse(5000)
  /** Configuration of external function libraries */
  lazy val functionLibrariesConfig: Option[FunctionLibrariesConfig] = Try(new FunctionLibrariesConfig(toFhirEngineConfig.getConfig("functionLibraries"))).toOption
}
