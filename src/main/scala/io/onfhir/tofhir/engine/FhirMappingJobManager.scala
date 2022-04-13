package io.onfhir.tofhir.engine

import io.onfhir.tofhir.model.{FhirMappingException, FhirMappingTask, FhirSinkSettings}
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
   * @param tasks          Mapping tasks that will be executed in sequential
   * @param sinkSettings   FHIR sink settings (can be a FHIR repository, file system, kafka)
   * @return
   */
  override def executeMappingJob(id: String, tasks: Seq[FhirMappingTask], sinkSettings: FhirSinkSettings): Future[Unit] = {
    val fhirWriter = FhirWriterFactory.apply(sinkSettings)
    tasks
      .map(t => executeTask(t)) // Retrieve the source data and execute the mapping
      .map { f => f.map(fhirResourcesDataSet => fhirWriter.write(fhirResourcesDataSet)) } // Write the created FHIR Resources to the FhirWriter
    match {
      case Seq(r) => r // Execute it if there is only a single Future
      case oth => oth.reduce((f1, f2) => f1.flatMap(_ => f2)) // Execute the Futures in the Sequence consecutively (not in parallel)
    }
  }

  /**
   * Execute a single mapping task.
   *
   * @param task A #FhirMappingTask to be executed.
   * @return
   */
  private def executeTask(task: FhirMappingTask): Future[DataFrame] = {
    //Retrieve the FHIR mapping definition
    val fhirMapping = fhirMappingRepository.getFhirMappingByUrl(task.mappingRef)
    val sourceNames = fhirMapping.source.map(_.alias).toSet
    val namesForSuppliedSourceContexts =  task.sourceContext.keySet
    if(sourceNames != namesForSuppliedSourceContexts)
      throw FhirMappingException(s"Invalid mapping task, source context is not given for some mapping source(s) ${sourceNames.diff(namesForSuppliedSourceContexts).mkString(", ")}")

    //Get the source schemas
    val sources = fhirMapping.source.map(s => (s.alias, schemaLoader.getSchema(s.url), task.sourceContext(s.alias)))
    //Read sources into Spark as DataFrame
    val sourceDataFrames =
      sources.map {
        case (alias, schema, sourceContext) =>
          alias ->
            DataSourceReaderFactory
              .apply(spark, sourceContext)
              .read(sourceContext, schema)
      }

    val df = handleJoin(task, sourceDataFrames)

    df.printSchema()

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
   * Handle the joining of source data framees
   * @param task              Mapping task definition
   * @param sourceDataFrames  Source data frames loaded
   * @return
   */
  def handleJoin(task: FhirMappingTask, sourceDataFrames:Seq[(String, DataFrame)]):DataFrame = {
    sourceDataFrames match {
      case Seq(_ -> df) => df
      case oth =>
        //Join of source data is not implemented yet
        throw new NotImplementedError()
    }
  }

  /**
   * Execute the given mapping job and return the resulting FHIR resources
   *
   * @param id             Unique job identifier
   * @param sourceSettings Data source settings and configurations
   * @param task           Mapping task that will be executed
   * @return
   */
  override def executeMappingTaskAndReturn(id: String,  task: FhirMappingTask): Future[Seq[JObject]] = {
    executeTask(task)
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
    fhirMappingService.sources match {
      case Seq(_) =>  executeMappingOnSingleSource(spark, df, fhirMappingService)
      //Executing on multiple sources
      case _ => throw new NotImplementedError()
    }
  }

  /**
   *
   * @param spark
   * @param df
   * @param fhirMappingService
   * @return
   */
  private def executeMappingOnSingleSource(spark: SparkSession, df: DataFrame, fhirMappingService: FhirMappingService):DataFrame = {
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
