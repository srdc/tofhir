package io.onfhir.tofhir.engine
import io.onfhir.tofhir.config.MappingErrorHandling.MappingErrorHandling
import io.onfhir.tofhir.data.write.FhirWriterFactory
import io.onfhir.tofhir.model.{FhirMappingTask, FhirSinkSettings}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery

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

  private def getStreamForMappingTask(task:FhirMappingTask):Dataset[String] = {
    //read data from kafka source reader

    //load context


    val fhirMappingService = new FhirMappingService(null, null, null)

    MappingTaskExecutor.executeMapping(spark, null, fhirMappingService, mappingErrorHandling)
  }

}
