package io.onfhir.tofhir

import io.onfhir.tofhir.config.MappingErrorHandling.MappingErrorHandling
import io.onfhir.tofhir.config.{MappingErrorHandling, ToFhirConfig}
import io.onfhir.tofhir.engine._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, Inspectors, OptionValues}

import java.net.URI

trait ToFhirTestSpec extends Matchers with OptionValues with Inside with Inspectors {

  val mappingErrorHandling: MappingErrorHandling = MappingErrorHandling.HALT
  val fhirWriteErrorHandling: MappingErrorHandling = MappingErrorHandling.HALT

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
