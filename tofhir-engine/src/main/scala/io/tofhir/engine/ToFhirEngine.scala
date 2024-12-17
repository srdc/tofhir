package io.tofhir.engine

import io.onfhir.path._
import io.tofhir.engine.config.{ToFhirConfig, ToFhirEngineConfig}
import io.tofhir.engine.execution.RunningJobRegistry
import io.tofhir.engine.execution.processing.FileStreamInputArchiver
import io.tofhir.engine.mapping.context.{IMappingContextLoader, MappingContextLoader}
import io.tofhir.engine.mapping.schema.{IFhirSchemaLoader, SchemaFolderLoader}
import io.tofhir.engine.model.exception.EngineInitializationException
import io.tofhir.engine.repository.mapping.{FhirMappingFolderRepository, IFhirMappingRepository}
import io.tofhir.engine.util.FileUtils
import org.apache.spark.sql.SparkSession

/**
 * <p>toFHIR Engine for executing mapping jobs and tasks.</p>
 * <p>During initialization, the engine prioritizes the mapping and schema repositories provided as a constructor parameter.
 * If they are not provided as constructor parameters, they are initialized as folder repository based on the folder paths set in the engine configurations.
 * </p>
 *
 * @param mappingRepository        Already instantiated mapping repository that maintains a dynamically-updated data structure based on the operations on the mappings
 * @param schemaRepository         Already instantiated schema repository that maintains a dynamically-updated data structure based on the operations on the schemas
 */
class ToFhirEngine(mappingRepository: Option[IFhirMappingRepository] = None, schemaRepository: Option[IFhirSchemaLoader] = None) {
  // Validate that both mapping and schema repositories are empty or non-empty
  if (mappingRepository.nonEmpty && schemaRepository.isEmpty || mappingRepository.isEmpty && schemaRepository.nonEmpty) {
    throw EngineInitializationException("Mapping and schema repositories should both empty or non-empty")
  }

  val engineConfig: ToFhirEngineConfig = ToFhirConfig.engineConfig

  val sparkSession: SparkSession = ToFhirConfig.sparkSession

  //Repository for mapping definitions
  val mappingRepo: IFhirMappingRepository = mappingRepository.getOrElse(new FhirMappingFolderRepository(FileUtils.getPath(engineConfig.mappingRepositoryFolderPath).toUri))

  //Context loader
  val contextLoader: IMappingContextLoader = new MappingContextLoader

  //Repository for source data schemas
  val schemaLoader: IFhirSchemaLoader = schemaRepository.getOrElse(new SchemaFolderLoader(FileUtils.getPath(engineConfig.schemaRepositoryFolderPath).toUri))

  // Function libraries containing context-independent, built-in libraries and libraries passed externally
  val functionLibraries: Map[String, IFhirPathFunctionLibraryFactory] = initializeFunctionLibraries()

  // Single registry keeping the running jobs
  val runningJobRegistry: RunningJobRegistry = new RunningJobRegistry(sparkSession)

  // Archiver for deleting or archiving the files processed
  val fileStreamInputArchiver: FileStreamInputArchiver = new FileStreamInputArchiver(runningJobRegistry)
  fileStreamInputArchiver.startStreamingArchiveTask()

  /**
   * Merges built-in function libraries and external libraries passed in the constructor
   *
   * @return
   */
  private def initializeFunctionLibraries(): Map[String, IFhirPathFunctionLibraryFactory] = {
    val externalFunctionLibraryFactories: Map[String, IFhirPathFunctionLibraryFactory] = engineConfig.functionLibrariesConfig
      .map(_.functionLibrariesFactories)
      .getOrElse(Map.empty)
    Map(
      FhirPathUtilFunctionsFactory.defaultPrefix -> FhirPathUtilFunctionsFactory,
      FhirPathNavFunctionsFactory.defaultPrefix -> FhirPathNavFunctionsFactory,
      FhirPathAggFunctionsFactory.defaultPrefix -> FhirPathAggFunctionsFactory,
      FhirPathIdentityServiceFunctionsFactory.defaultPrefix -> FhirPathIdentityServiceFunctionsFactory,
      FhirPathTerminologyServiceFunctionsFactory.defaultPrefix -> FhirPathTerminologyServiceFunctionsFactory
    ) ++ externalFunctionLibraryFactories
  }
}
