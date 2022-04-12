package io.onfhir.tofhir.engine

import io.onfhir.api.Resource
import io.onfhir.client.OnFhirNetworkClient
import io.onfhir.tofhir.model.FhirRepositorySinkSettings
import org.apache.spark.sql.{DataFrame, Row}

import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration

/**
 * Class to write the dataset to given FHIR repository
 *
 * @param sinkSettings Settings for the FHIR repository
 */
class FhirRepositoryWriter(sinkSettings: FhirRepositorySinkSettings) extends BaseFhirWriter(sinkSettings) {

  /**
   * Write the data frame of FHIR resources to given FHIR repository
   *
   * @param df
   */
  override def write(df: DataFrame): Unit = {
    println("111111111111111111111")

    df.cache()
    df.printSchema()

    import io.onfhir.util.JsonFormatter._

    df.foreach { r: Row =>
      println(MappingTaskExecutor.convertRowToJObject(r).toJson)
    }

//    df.foreachPartition { partition: Iterator[Row] =>
//      val onFhirClient = OnFhirNetworkClient.apply(sinkSettings.fhirRepoUrl) // A FhirClient for each partition
//      partition.grouped(10)
//        .foreach(rowGroup => {
//          var batchRequest = onFhirClient.batch()
//          rowGroup.foreach(row => {
//            batchRequest = batchRequest.entry(_.create(row.asInstanceOf[Resource]))
//          })
//          val a = Await.result(batchRequest.execute(), FiniteDuration(5, TimeUnit.SECONDS))
//          println(a.responseBody.get)
//        })
//    }
  }
}
