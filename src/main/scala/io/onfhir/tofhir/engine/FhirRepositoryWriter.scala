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
    df.cache()
    df.printSchema()

    import io.onfhir.util.JsonFormatter._

    df
      .toJSON
      .foreachPartition { partition: Iterator[String] =>
      val onFhirClient = OnFhirNetworkClient.apply(sinkSettings.fhirRepoUrl) // A FhirClient for each partition
      partition
        .grouped(10)
        .foreach(rowGroup => {
          var batchRequest = onFhirClient.batch()
          rowGroup.foreach(row => {
            val resource = row.parseJson
            batchRequest = batchRequest.entry(_.update(resource))
          })
          val a = Await.result(batchRequest.execute(), FiniteDuration(5, TimeUnit.SECONDS))
          println(a.responseBody.get)
        })
    }
  }
}
