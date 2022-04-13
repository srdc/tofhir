package io.onfhir.tofhir.engine

import io.onfhir.tofhir.model.{DataSourceSettings, FhirMappingTask, FhirSinkSettings}
import io.onfhir.util.JsonFormatter._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.json4s.JObject

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/**
 * Main entrypoint for FHIR mapping jobs
 *
 * @param fhirMappingRepository Repository for mapping definitions
 * @param contextLoader         Context loader
 * @param schemaLoader          Schema (StructureDefinition) loader
 * @param spark                 Spark session
 * @param ec
 */
class FhirMappingJobManager(
                             fhirMappingRepository: IFhirMappingRepository,
                             contextLoader: IMappingContextLoader,
                             schemaLoader:IFhirSchemaLoader,
                             spark: SparkSession
                           )(implicit ec: ExecutionContext) extends IFhirMappingJobManager {

  /**
   * Execute the given mapping job and write the resulting FHIR resources to given sink
   *
   * @param id             Unique job identifier
   * @param sourceSettings Data source settings and configurations
   * @param tasks          Mapping tasks that will be executed in sequential
   * @param sinkSettings   FHIR sink settings (can be a FHIR repository, file system, kafka)
   * @return
   */
  override def executeMappingJob[T <: FhirMappingTask](id: String, sourceSettings: DataSourceSettings[T], tasks: Seq[T], sinkSettings: FhirSinkSettings): Future[Unit] = {
    val dataSourceReader = DataSourceReaderFactory.apply(spark, sourceSettings)
    val fhirWriter = FhirWriterFactory.apply(sinkSettings)
    tasks
      .map(t => executeTask(dataSourceReader, t)) // Retrieve the source data and execute the mapping
      .map { f => f.map(fhirResourcesDataSet => fhirWriter.write(fhirResourcesDataSet)) } // Write the created FHIR Resources to the FhirWriter
    match {
      case Seq(r) => r // Execute it if there is only a single Future
      case oth => oth.reduce((f1, f2) => f1.flatMap(_ => f2)) // Execute the Futures in the Sequence consecutively (not in parallel)
    }
  }

  /**
   * Execute a single mapping task.
   *
   * @param dataSourceReader A #BaseDataSourceReader to read the source data.
   * @param task A #FhirMappingTask to be executed.
   * @return
   */
  private def executeTask[T <: FhirMappingTask](dataSourceReader: BaseDataSourceReader[T], task: T): Future[DataFrame] = {
    //Retrieve the FHIR mapping definition
    val fhirMapping = fhirMappingRepository.getFhirMappingByUrl(task.mappingRef)

    val df = dataSourceReader.read(task)
    //    df.cache()
    //    df.printSchema()

    //Load the contextual data for the mapping
    Future
      .sequence(
        fhirMapping
          .context
          .toSeq
          .map(cdef => contextLoader.retrieveContext(cdef._2).map(context => cdef._1 -> context))
      ).map(loadedContextMap => {
      //Construct the mapping service
      val fhirMappingService = new FhirMappingService(fhirMapping.source.map(_.alias), loadedContextMap.toMap, fhirMapping.mapping)
      MappingTaskExecutor.executeMapping(spark, df, fhirMappingService)
    })
  }

  /**
   * Execute the given mapping job and return the resulting FHIR resources
   *
   * @param id             Unique job identifier
   * @param sourceSettings Data source settings and configurations
   * @param task           Mapping task that will be executed
   * @return
   */
  override def executeMappingTaskAndReturn[T <: FhirMappingTask](id: String, sourceSettings: DataSourceSettings[T], task: T): Future[Seq[JObject]] = {
    val dataSourceReader = DataSourceReaderFactory.apply(spark, sourceSettings)
    executeTask(dataSourceReader, task)
      .map { dataFrame =>
        dataFrame
          .toJSON // Convert to Dataset[String] where the Strings are JSON Strings
          .collect() // Collect into an Array[String]
          .map(_.parseJson) // Parse each JSON String into FHIR Resource where Resource is a JObject
          .toSeq // Convert to Seq[Resource]
      }
  }
}

object MappingTaskExecutor {

  /**
   * Convert input row for mapping to JObject
   *
   * @param row
   * @return
   */
  def convertRowToJObject(row: Row): JObject = {
    // Row class of Spark has a private method (jsonValue) which transforms a Row into a json4s JObject.
    // We hacked the class to make it accessible and used it to covert a Row into JObject.
    val method = row.getClass.getSuperclass.getInterfaces.apply(0).getMethod("jsonValue")
    method.setAccessible(true)
    method.invoke(row).asInstanceOf[JObject]
  }

  /**
   * Executing the mapping and returning the dataframe for FHIR resources
   *
   * @param spark
   * @param df
   * @param fhirMappingService
   * @return
   */
  def executeMapping(spark: SparkSession, df: DataFrame, fhirMappingService: FhirMappingService): DataFrame = {
    import spark.implicits._
    val result =
      df
        .flatMap(row =>
          Await.result(fhirMappingService.mapToFhir(convertRowToJObject(row)), Duration.apply(5, "seconds"))
            .map(_.toJson)
        )

    spark.read.json(result)
  }
}
