package io.onfhir.tofhir.config

import com.typesafe.config.{Config, ConfigFactory}

import scala.util.Try

object ToFhirConfig {

  protected val config: Config = ConfigFactory.load()

  /** Application name for Spark */
  lazy val appName: String = Try(config.getString("spark.app-name")).getOrElse("AICCELERATE Data Integration Suite")

  /** Master url of the Spark cluster */
  lazy val sparkMaster: String = Try(config.getString("spark.master")).getOrElse("local[4]")

  /** Absolute file path to the MappingJobs file while initiating the Data Integration Suite */
  lazy val mappingJobsFilePath: Option[String] = Try(config.getString("mapping-jobs.file-path")).toOption

  /** The # of FHIR resources in the group while executing (create/update) a batch operation. */
  lazy val fhirWriterBatchGroupSize: Int = Try(config.getInt("fhir-writer.batch-group-size")) .getOrElse(10)

  /** Error handling mechanism of the mappings to indicate whether  */
  lazy val mappingErrorHandling: String = Try(config.getString("mapping.error-handling")).getOrElse(MAPPING_ERROR_HANDLING.CONTINUE)

}
