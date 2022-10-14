package io.onfhir.tofhir.config

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import collection.JavaConverters._
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.util.Try
import scala.jdk.DurationConverters._

object ToFhirConfig {

  protected val config: Config = ConfigFactory.load()

  /**
   * Tofhir specific configurations
   */
  lazy val toFhirConfig = config.getConfig("tofhir")

  /** Path to the folder where the mappings are kept. */
  lazy val mappingRepositoryFolderPath: String = Try(toFhirConfig.getString("mappings.repository.folder-path")).getOrElse("mappings")

  /** Path to the folder where the schema definitions are kept. */
  lazy val schemaRepositoryFolderPath: String = Try(toFhirConfig.getString("mappings.schemas.repository.folder-path")).getOrElse("schemas")

  /**
   * Timeout for a single mapping
   */
  lazy val mappingTimeout: Duration = Try(toFhirConfig.getDuration("mappings.timeout").toScala).toOption.getOrElse(Duration.apply(5, TimeUnit.SECONDS))

  /** Absolute file path to the MappingJobs file while initiating the Data Integration Suite */
  lazy val mappingJobFilePath: Option[String] = Try(toFhirConfig.getString("initial-job-file-path ")).toOption

  /** The # of FHIR resources in the group while executing (create/update) a batch operation. */
  lazy val fhirWriterBatchGroupSize: Int = Try(toFhirConfig.getInt("fhir-server-writer.batch-group-size")).getOrElse(10)

  /** Path to the folder where the execution times of scheduled mapping jobs are kept. */
  lazy val toFhirDb: Option[String] = Try(toFhirConfig.getString("db")).toOption


  /**
   * Spark configurations
   */
  lazy val sparkConfig = config.getConfig("spark")
  /** Application name for Spark */
  lazy val sparkAppName: String = Try(sparkConfig.getString("app.name")).getOrElse("AICCELERATE Data Integration Suite")
  /** Master url of the Spark cluster */
  lazy val sparkMaster: String = Try(sparkConfig.getString("master")).getOrElse("local[4]")

  /**
   * Default configurations for spark
   */
  val sparkConfDefaults: Map[String, String] =
    Map(
      "spark.driver.allowMultipleContexts" -> "false",
      "spark.ui.enabled" -> "false",
      "spark.sql.files.ignoreCorruptFiles" -> "false", //Do not ignore corrupted files (e.g. CSV missing a field from the given schema) as we want to log them
      "spark.sql.streaming.checkpointLocation" -> "./checkpoint" //Checkpoint directory for streaming
    )

  /**
   * Create spark configuration from the given config
   *
   * @return
   */
  def createSparkConf: SparkConf = {
    val sparkConf = new SparkConf()
      .setAppName(sparkAppName)
      .setMaster(sparkMaster)

    val sparkConfEntries =
      sparkConfDefaults ++ //Defaults plus provided entries
        sparkConfig
          .entrySet()
          .asScala
          .filter(e => e.getKey != "app.name" && e.getKey != "master")
          .map(e => e.getKey -> e.getValue.render())
          .toMap

    sparkConfEntries
      .foldLeft(sparkConf) {
        case (sc, e) => sc.set(e._1, e._2)
      }
  }
}
