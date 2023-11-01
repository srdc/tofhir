package io.tofhir.log.server.config

import com.typesafe.config.Config
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.Try

/**
 * Keeps the configuration of Spark
 * */
object SparkConfig {

  import io.tofhir.engine.Execution.actorSystem

  protected lazy val config: Config = actorSystem.settings.config

  /**
   * Spark configurations
   */
  lazy val sparkConfig: Config = config.getConfig("spark")
  /** Application name for Spark */
  lazy val sparkAppName: String = Try(sparkConfig.getString("app.name")).getOrElse("AICCELERATE Data Integration Suite")
  /** Master url of the Spark cluster */
  lazy val sparkMaster: String = Try(sparkConfig.getString("master")).getOrElse("local[4]")
  // Spark session
  lazy val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  // Spark configurations for starting a Spark session
  private lazy val sparkConf: SparkConf = new SparkConf()
    .setAppName(sparkAppName)
    .setMaster(sparkMaster)
}
