package io.onfhir.tofhir.engine

import io.onfhir.tofhir.model.{DataSourceSettings, FhirMappingTask, FhirSinkSettings, MappedFhirResource}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.json4s.JObject

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/**
 * Main entrypoint for FHIR mapping jobs
 * @param fhirMappingRepository   Repository for mapping definitions
 * @param contextLoader           Context loader
 * @param spark                   Spark session
 * @param ec
 */
class FhirMappingJobManager(
                              fhirMappingRepository:IFhirMappingRepository,
                              contextLoader:IMappingContextLoader,
                              spark:SparkSession
                           )(implicit ec:ExecutionContext) extends IFhirMappingJobManager {
  /**
   * Execute the given mapping job and write the resulting FHIR resources to given sink
   *
   * @param id             Unique job identifier
   * @param sourceSettings Data source settings and configurations
   * @param tasks          Mapping tasks that will be executed in sequential
   * @param sinkSettings   FHIR sink settings (can be a FHIR repository, file system, kafka)
   * @return
   */
  override def executeMappingJob[T<:FhirMappingTask](id: String, sourceSettings: DataSourceSettings[T], tasks: Seq[T], sinkSettings: FhirSinkSettings): Future[Unit] = {
    val dataSourceReader = DataSourceReaderFactory.apply(spark, sourceSettings)
    val fhirWriter = FhirWriterFactory.apply(sinkSettings)
    tasks
      .map(t => executeTask(dataSourceReader, t)) //Retrieve the source data and execute the mapping
      .map(_.map(fhirResourcesDataSet => fhirWriter.write(fhirResourcesDataSet))) match {
        case Seq(r) => r
        case oth => oth.reduce((f1, f2) => f1.flatMap(_ => f2))
    }
  }

  /**
   *
   * @param dataSourceReader
   * @param task
   * @param sinkSettings
   * @return
   */
  private def executeTask[T<:FhirMappingTask](dataSourceReader:BaseDataSourceReader[T], task:T):Future[Dataset[MappedFhirResource]] = {
    val df = dataSourceReader.read(task)
    df.cache()
    //Retrieve the FHIR mapping definition
    val fhirMapping = fhirMappingRepository.getFhirMappingByUrl(task.mappingRef)
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
   * @param id              Unique job identifier
   * @param sourceSettings  Data source settings and configurations
   * @param task            Mapping task that will be executed
   * @return
   */
  override def executeMappingTaskAndReturn[T<:FhirMappingTask](id:  String, sourceSettings:  DataSourceSettings[T], task: T): Future[Seq[JObject]] = {
    import spark.implicits._
    val dataSourceReader = DataSourceReaderFactory.apply(spark, sourceSettings)
    executeTask(dataSourceReader, task)
      .map(r => r.map(_.resource).collect().toSeq)
  }
}

object MappingTaskExecutor {

  private def convertRowToJObject(row: Row): JObject = {
    throw new NotImplementedError()
  }

  def executeMapping(spark:SparkSession, df:DataFrame, fhirMappingService: FhirMappingService):Dataset[MappedFhirResource] = {
    import spark.implicits._
    df.flatMap(row =>
      Await.result(fhirMappingService.mapToFhir(convertRowToJObject(row)), Duration.apply(5, "seconds"))
    )
  }
}
