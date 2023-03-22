package io.tofhir.engine

import io.tofhir.engine.config.ToFhirConfig
import io.tofhir.engine.mapping._
import io.tofhir.engine.util.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * tofhir Engine for executing mapping jobs and tasks
 * @param appName                  Name of the application
 * @param sparkMaster
 * @param repositoryFolderPath
 * @param schemaRepositoryPath
 */
class ToFhirEngine(appName: String, sparkMaster: String, repositoryFolderPath: String, schemaRepositoryPath: String) {
  //Spark configurations
  private val sparkConf: SparkConf = ToFhirConfig.createSparkConf

  val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  //Repository for mapping definitions
  val mappingRepository: IFhirMappingCachedRepository = new FhirMappingFolderRepository(FileUtils.getPath(repositoryFolderPath).toUri)

  //Context loader
  val contextLoader: IMappingContextLoader = new MappingContextLoader(mappingRepository)

  //Repository for source data schemas
  val schemaLoader = new SchemaFolderLoader(FileUtils.getPath(schemaRepositoryPath).toUri)
}
