package io.onfhir.tofhir.config

import com.typesafe.config.{Config, ConfigFactory}

import scala.util.Try

object ToFhirConfig {

  protected val config: Config = ConfigFactory.load()

  /** Application name for Spark */
  lazy val appName: String = Try(config.getString("spark.app-name")).getOrElse("AICCELERATE Data Integration Suite")

  /** Master url of the Spark cluster */
  lazy val sparkMaster: String = Try(config.getString("spark.master")).getOrElse("local[4]")

  /** Path to the folder where the mappings are kept. */
  lazy val mappingRepositoryFolderPath: String = Try(config.getString("mappings.repository.folder-path")).getOrElse("mappings")

  /** Path to the folder where the schema definitions are kept. */
  lazy val schemaRepositoryFolderPath: String = Try(config.getString("mappings.schemas.repository.folder-path")).getOrElse("schemas")

  /** Error handling mechanism of the mappings to indicate whether  */
  lazy val mappingErrorHandling: String = Try(config.getString("mapping.error-handling")).getOrElse(MappingErrorHandling.CONTINUE.toString)

  /** Absolute file path to the MappingJobs file while initiating the Data Integration Suite */
  lazy val mappingJobFilePath: Option[String] = Try(config.getString("mapping-job.file-path")).toOption

  /** The # of FHIR resources in the group while executing (create/update) a batch operation. */
  lazy val fhirWriterBatchGroupSize: Int = Try(config.getInt("fhir-writer.batch-group-size")) .getOrElse(10)

}
