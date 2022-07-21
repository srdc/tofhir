package io.onfhir.tofhir

import io.onfhir.tofhir.engine._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.nio.file.Paths

/**
 * tofhir Engine for executing mapping jobs and tasks
 * @param appName                  Name of the application
 * @param sparkMaster
 * @param repositoryFolderPath
 * @param schemaRepositoryPath
 */
class ToFhirEngine(appName: String, sparkMaster: String, repositoryFolderPath: String, schemaRepositoryPath: String) {
  //Spark configurations
  private val sparkConf: SparkConf = new SparkConf()
    .setAppName(appName)
    .setMaster(sparkMaster)
    .set("spark.driver.allowMultipleContexts", "false")
    .set("spark.ui.enabled", "false")

  val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  //Repository for mapping definitions
  val mappingRepository: IFhirMappingRepository = new FhirMappingFolderRepository(Paths.get(repositoryFolderPath).toUri)

  //Context loader
  val contextLoader: IMappingContextLoader = new MappingContextLoader(mappingRepository)

  //Repository for source data schemas
  val schemaRepository = new SchemaFolderRepository(Paths.get(schemaRepositoryPath).toUri)
}
