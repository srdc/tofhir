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

}
