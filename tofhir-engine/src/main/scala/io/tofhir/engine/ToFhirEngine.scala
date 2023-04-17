package io.tofhir.engine

import io.tofhir.engine.config.{ToFhirConfig, ToFhirEngineConfig}
import io.tofhir.engine.mapping._
import io.tofhir.engine.model.EngineInitializationException
import io.tofhir.engine.util.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * <p>tofhir Engine for executing mapping jobs and tasks.</p>
 * <p>During initialization, the engine prioritizes the mapping and schema repositories provided as a constructor parameter.
 * If they are not provided as constructor parameters, they are initialized as folder repository based on the folder paths set in the engine configurations.
 * </p>
 *
 * @param mappingRepo
 * @param schemaRepository
 */
class ToFhirEngine(mappingRepo: Option[IFhirMappingCachedRepository] = None, schemaRepository: Option[IFhirSchemaLoader] = None) {
  // Validate that both mapping and schema repositories are empty or non-empty
  if (mappingRepo.nonEmpty && schemaRepository.isEmpty || mappingRepo.isEmpty && schemaRepository.nonEmpty) {
    throw EngineInitializationException("Mapping and schema repositories should both empty or non-empty")
  }

  //Spark configurations
  private val sparkConf: SparkConf = ToFhirConfig.createSparkConf
  val engineConfig: ToFhirEngineConfig = ToFhirConfig.engineConfig

  val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  //Repository for mapping definitions
  val mappingRepository: IFhirMappingCachedRepository = mappingRepo.getOrElse(new FhirMappingFolderRepository(FileUtils.getPath(engineConfig.mappingRepositoryFolderPath).toUri))

  //Context loader
  val contextLoader: IMappingContextLoader = new MappingContextLoader(mappingRepository)

  //Repository for source data schemas
  val schemaLoader: IFhirSchemaLoader = schemaRepository.getOrElse(new SchemaFolderLoader(FileUtils.getPath(engineConfig.schemaRepositoryFolderPath).toUri))
}
