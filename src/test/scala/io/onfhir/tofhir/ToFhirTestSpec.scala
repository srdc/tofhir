package io.onfhir.tofhir

import io.onfhir.tofhir.config.ToFhirConfig
import io.onfhir.tofhir.engine.{FhirMappingFolderRepository, IFhirMappingRepository, IMappingContextLoader, MappingContextLoader, SchemaFolderRepository}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{Inside, Inspectors, OptionValues}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

import java.net.URI

abstract class ToFhirTestSpec extends AsyncFlatSpec with should.Matchers with
  OptionValues with Inside with Inspectors {

  val repositoryFolderUri: URI = getClass.getResource(ToFhirConfig.mappingRepositoryFolderPath).toURI
  val mappingRepository: IFhirMappingRepository = new FhirMappingFolderRepository(repositoryFolderUri)

  val contextLoader: IMappingContextLoader = new MappingContextLoader(mappingRepository)

  val schemaRepositoryURI: URI = getClass.getResource(ToFhirConfig.schemaRepositoryFolderPath).toURI
  val schemaRepository = new SchemaFolderRepository(schemaRepositoryURI)

  val sparkConf: SparkConf = new SparkConf()
    .setAppName(ToFhirConfig.appName)
    .setMaster(ToFhirConfig.sparkMaster)
    .set("spark.driver.allowMultipleContexts", "false")
    .set("spark.ui.enabled", "false")
  val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
}
