package io.tofhir

import akka.actor.ActorSystem
import io.tofhir.engine.config.ErrorHandlingType.ErrorHandlingType
import io.tofhir.engine.config.{ErrorHandlingType, ToFhirConfig}
import io.tofhir.engine.mapping.{FhirMappingFolderRepository, IFhirMappingRepository, IMappingContextLoader, MappingContextLoader, SchemaFolderLoader}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, Inspectors, OptionValues}

import java.net.URI

trait ToFhirTestSpec extends Matchers with OptionValues with Inside with Inspectors {

  val mappingErrorHandling: ErrorHandlingType = ErrorHandlingType.HALT
  val fhirWriteErrorHandling: ErrorHandlingType = ErrorHandlingType.HALT

  val repositoryFolderUri: URI = getClass.getResource(ToFhirConfig.engineConfig.mappingRepositoryFolderPath).toURI
  val mappingRepository: IFhirMappingRepository = new FhirMappingFolderRepository(repositoryFolderUri)

  val contextLoader: IMappingContextLoader = new MappingContextLoader(mappingRepository)

  val schemaRepositoryURI: URI = getClass.getResource(ToFhirConfig.engineConfig.schemaRepositoryFolderPath).toURI
  val schemaRepository = new SchemaFolderLoader(schemaRepositoryURI)

  val sparkConf: SparkConf = new SparkConf()
    .setAppName(ToFhirConfig.sparkAppName)
    .setMaster(ToFhirConfig.sparkMaster)
    .set("spark.driver.allowMultipleContexts", "false")
    .set("spark.ui.enabled", "false")
  val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  implicit val actorSystem: ActorSystem = ActorSystem("toFhirEngineTest")
}
