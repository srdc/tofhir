package io.tofhir.engine.config

import com.typesafe.config.Config
import io.tofhir.engine.util.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

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
  private lazy val sparkConfig: Config = config.getConfig("spark")
  /** Application name for Spark */
  private lazy val sparkAppName: String = Try(sparkConfig.getString("app.name")).getOrElse("AICCELERATE Data Integration Suite")
  /** Master url of the Spark cluster */
  private lazy val sparkMaster: String = Try(sparkConfig.getString("master")).getOrElse("local[4]")
  /** Directory to keep Spark's checkpoints created  */
  lazy val sparkCheckpointDirectory: String = FileUtils.getPath(Try(sparkConfig.getString("checkpoint-dir")).getOrElse("checkpoint")).toString
  /**
   * Default configurations for spark
   */
  private val sparkConfDefaults: Map[String, String] =
    Map(
      "spark.driver.allowMultipleContexts" -> "false",
      "spark.sql.caseSensitive" -> "true", // Enable case sensitivity to treat schema column names as case-sensitive to avoid potential conflicts
      "spark.sql.files.ignoreCorruptFiles" -> "false", //Do not ignore corrupted files (e.g. CSV missing a field from the given schema) as we want to log them
      "spark.sql.streaming.checkpointLocation" -> sparkCheckpointDirectory, //Checkpoint directory for streaming
      "spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs" -> "false", //Do not create _SUCCESS file while writing to csv
      "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension", // Enable Delta Lake features by adding the DeltaSparkSessionExtension to the Spark session.
      "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog" // Use DeltaCatalog as the default catalog for managing Delta tables in Spark.
    )
  // Spark session
  lazy val sparkSession: SparkSession = SparkSession.builder().config(createSparkConf).getOrCreate()
  /**
   * Create spark configuration from this config
   */
  private def createSparkConf: SparkConf = {
    val sparkConf = new SparkConf()
      .setAppName(sparkAppName)
      .setMaster(sparkMaster)

    val sparkConfEntries =
      sparkConfDefaults ++ //Defaults plus provided entries
        sparkConfig
          .entrySet()
          .asScala
          .filter(e => e.getKey != "app.name" && e.getKey != "master")
          .map(e => s"spark.${e.getKey}" -> e.getValue.unwrapped().toString)
          .toMap

    sparkConfEntries
      .foldLeft(sparkConf) {
        case (sc, e) => sc.set(e._1, e._2)
      }
  }
}
