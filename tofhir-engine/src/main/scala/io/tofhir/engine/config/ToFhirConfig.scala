package io.tofhir.engine.config

import com.typesafe.config.Config
import org.apache.spark.SparkConf

import scala.jdk.CollectionConverters._
import scala.util.Try

object ToFhirConfig {

  import io.tofhir.engine.Execution.actorSystem
  protected lazy val config: Config = actorSystem.settings.config

  /**
   * ToFhir Engine configurations
   */
  lazy val engineConfig = new ToFhirEngineConfig(config.getConfig("tofhir"))

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
   * Create spark configuration from this config
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
