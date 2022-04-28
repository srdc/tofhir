package io.onfhir.tofhir

import io.onfhir.tofhir.config.MappingErrorHandling
import io.onfhir.tofhir.config.MappingErrorHandling.MappingErrorHandling
import io.onfhir.tofhir.engine._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.nio.file.Paths

class ToFhirEngine(appName: String, sparkMaster: String, repositoryFolderPath: String, schemaRepositoryPath: String, val errorHandling: MappingErrorHandling = MappingErrorHandling.CONTINUE) {

  private val sparkConf: SparkConf = new SparkConf()
    .setAppName(appName)
    .setMaster(sparkMaster)
    .set("spark.driver.allowMultipleContexts", "false")
    .set("spark.ui.enabled", "false")

  val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  val mappingRepository: IFhirMappingRepository = new FhirMappingFolderRepository(Paths.get(repositoryFolderPath).toUri)

  val contextLoader: IMappingContextLoader = new MappingContextLoader(mappingRepository)

  val schemaRepository = new SchemaFolderRepository(Paths.get(schemaRepositoryPath).toUri)

}
