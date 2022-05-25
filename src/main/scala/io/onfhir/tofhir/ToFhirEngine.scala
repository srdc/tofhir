package io.onfhir.tofhir

import io.onfhir.tofhir.engine._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.nio.file.Paths

class ToFhirEngine(appName: String, sparkMaster: String, repositoryFolderPath: String, schemaRepositoryPath: String) {

  private val sparkConf: SparkConf = new SparkConf()
    .setAppName(appName)
    .setMaster(sparkMaster)
    .set("spark.driver.allowMultipleContexts", "false")
    .set("spark.ui.enabled", "false")

  val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  def mappingRepository: IFhirMappingRepository = new FhirMappingFolderRepository(Paths.get(repositoryFolderPath).toUri)

  val contextLoader: IMappingContextLoader = new MappingContextLoader(mappingRepository)

  def schemaRepository = new SchemaFolderRepository(Paths.get(schemaRepositoryPath).toUri)

}
