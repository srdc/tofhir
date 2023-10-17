package io.tofhir.engine

import io.onfhir.path._
import io.tofhir.engine.config.{ToFhirConfig, ToFhirEngineConfig}
import io.tofhir.engine.execution.{FileStreamInputArchiver, RunningJobRegistry}
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
 * @param mappingRepository        Already instantiated mapping repository that maintains a dynamically-updated data structure based on the operations on the mappings
 * @param schemaRepository         Already instantiated schema repository that maintains a dynamically-updated data structure based on the operations on the schemas
 * @param functionLibraryFactories External function libraries containing function to be used within FHIRPath expressions
 */
class ToFhirEngine(mappingRepository: Option[IFhirMappingCachedRepository] = None, schemaRepository: Option[IFhirSchemaLoader] = None, functionLibraryFactories: Map[String, IFhirPathFunctionLibraryFactory] = Map.empty) {
  // Validate that both mapping and schema repositories are empty or non-empty
  if (mappingRepository.nonEmpty && schemaRepository.isEmpty || mappingRepository.isEmpty && schemaRepository.nonEmpty) {
    throw EngineInitializationException("Mapping and schema repositories should both empty or non-empty")
  }

  //Spark configurations
  private val sparkConf: SparkConf = ToFhirConfig.createSparkConf

  val engineConfig: ToFhirEngineConfig = ToFhirConfig.engineConfig

  val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  //Repository for mapping definitions
  val mappingRepo: IFhirMappingCachedRepository = mappingRepository.getOrElse(new FhirMappingFolderRepository(FileUtils.getPath(engineConfig.mappingRepositoryFolderPath).toUri))

  //Context loader
  val contextLoader: IMappingContextLoader = new MappingContextLoader(mappingRepo)

  //Repository for source data schemas
  val schemaLoader: IFhirSchemaLoader = schemaRepository.getOrElse(new SchemaFolderLoader(FileUtils.getPath(engineConfig.schemaRepositoryFolderPath).toUri))

  // Function libraries containing context-independent, built-in libraries and libraries passed externally
  val functionLibraries: Map[String, IFhirPathFunctionLibraryFactory] = initializeFunctionLibraries()

  // Single registry keeping the running jobs
  val runningJobRegistry: RunningJobRegistry = new RunningJobRegistry(sparkSession)

  // Archiver for deleting or archiving the files processed
  val fileStreamInputArchiver: FileStreamInputArchiver = new FileStreamInputArchiver(runningJobRegistry)
  fileStreamInputArchiver.startProcessorTask()

  /**
   * Merges built-in function libraries and external libraries passed in the constructor
   *
   * @return
   */
  private def initializeFunctionLibraries(): Map[String, IFhirPathFunctionLibraryFactory] = {
    Map(
      FhirPathUtilFunctionsFactory.defaultPrefix -> FhirPathUtilFunctionsFactory,
      FhirPathNavFunctionsFactory.defaultPrefix -> FhirPathNavFunctionsFactory,
      FhirPathAggFunctionsFactory.defaultPrefix -> FhirPathAggFunctionsFactory,
      FhirPathIdentityServiceFunctionsFactory.defaultPrefix -> FhirPathIdentityServiceFunctionsFactory,
      FhirPathTerminologyServiceFunctionsFactory.defaultPrefix -> FhirPathTerminologyServiceFunctionsFactory
    ) ++ functionLibraryFactories
  }
}
