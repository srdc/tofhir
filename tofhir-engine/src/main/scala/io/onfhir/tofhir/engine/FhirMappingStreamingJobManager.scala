package io.onfhir.tofhir.engine
import io.onfhir.tofhir.config.MappingErrorHandling.MappingErrorHandling
import io.onfhir.tofhir.data.read.DataSourceReaderFactory
import io.onfhir.tofhir.data.write.FhirWriterFactory
import io.onfhir.tofhir.engine.Execution.actorSystem.dispatcher
import io.onfhir.tofhir.model.{FhirMappingException, FhirMappingTask, FhirSinkSettings}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class FhirMappingStreamingJobManager(fhirMappingRepository: IFhirMappingRepository,
                                     contextLoader: IMappingContextLoader,
                                     schemaLoader: IFhirSchemaLoader,
                                     spark: SparkSession,
                                     mappingErrorHandling: MappingErrorHandling) extends IFhirMappingStreamingJobManager {
  /**
   * Start streaming mapping job
   *
   * @param id           Job identifier
   * @param tasks        Mapping tasks that will be executed in parallel in stream mode
   * @param sinkSettings FHIR sink settings (can be a FHIR repository, file system, kafka)
   * @return
   */
  override def executeMappingJob(id: String, tasks: Seq[FhirMappingTask], sinkSettings: FhirSinkSettings): StreamingQuery = {
    val fhirWriter = FhirWriterFactory.apply(sinkSettings)
    val mappedResourcesDf =
      tasks
      .map(t => getStreamForMappingTask(t))
      .reduce((ts1, ts2) => ts1.union(ts2))

    val datasetWrite = (dataset:Dataset[String], batchN:Long) => fhirWriter.write(dataset)

    mappedResourcesDf
      .writeStream
      .foreachBatch(datasetWrite)
      .start()
  }

  /**
   * Return dataset for each mapping task
   *
   * @param task FhirMappingTask to be mapped
   * @return
   */

  private def getStreamForMappingTask(task:FhirMappingTask):Dataset[String] = {
    //read data from kafka source reader
    val fhirMapping = fhirMappingRepository.getFhirMappingByUrl(task.mappingRef)
    val sourceNames = fhirMapping.source.map(_.alias).toSet
    val namesForSuppliedSourceContexts = task.sourceContext.keySet
    if (sourceNames != namesForSuppliedSourceContexts)
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

//    sourceDataFrames.head._2.printSchema()

    //load context
    val x = Future
      .sequence(
        fhirMapping
          .context
          .toSeq
          .map(cdef => contextLoader.retrieveContext(cdef._2).map(context => cdef._1 -> context))
      ).map(loadedContextMap => {
      //Get configuration context
      val sourceNames = fhirMapping.source.map(_.alias)
      val configurationContext = task.sourceContext(sourceNames.head).settings.toConfigurationContext
      //Construct the mapping service
      val fhirMappingService = new FhirMappingService(fhirMapping.source.map(_.alias), (loadedContextMap :+ configurationContext).toMap, fhirMapping.mapping)
      MappingTaskExecutor.executeMapping(spark, sourceDataFrames.head._2, fhirMappingService, mappingErrorHandling)
    })
    Await.result(x, Duration.Inf)
  }

}
