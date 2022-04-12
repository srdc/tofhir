package io.onfhir.tofhir.engine

import com.typesafe.scalalogging.Logger
import io.onfhir.client.OnFhirNetworkClient
import io.onfhir.tofhir.model.{FhirMappingException, FhirRepositorySinkSettings}
import org.apache.spark.sql.DataFrame

import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration

/**
 * Class to write the dataset to given FHIR repository
 *
 * @param sinkSettings Settings for the FHIR repository
 */
class FhirRepositoryWriter(sinkSettings: FhirRepositorySinkSettings) extends BaseFhirWriter(sinkSettings) {

  private val logger: Logger = Logger(this.getClass)

  val BATCH_GROUP_SIZE: Int = 3

  /**
   * Write the data frame of FHIR resources to given FHIR repository
   *
   * @param df
   */
  override def write(df: DataFrame): Unit = {
    logger.debug("Created FHIR resources will be written to the given FHIR repository URL:{}", sinkSettings.fhirRepoUrl)
    import io.onfhir.util.JsonFormatter._
    df
      .toJSON // Convert all Rows of this DataFrame to JSON String
      .foreachPartition { partition: Iterator[String] =>
      val onFhirClient = OnFhirNetworkClient.apply(sinkSettings.fhirRepoUrl) // A FhirClient for each partition
      partition
        .grouped(BATCH_GROUP_SIZE)
        .foreach(rowGroup => {
          var batchRequest = onFhirClient.batch()
          rowGroup.foreach(row => {
            val resource = row.parseJson
            batchRequest = batchRequest.entry(_.update(resource))
          })
          logger.debug("Batch Update request will be sent to the FHIR repository for {} resources.", rowGroup.size)
          val response = Await.result(batchRequest.execute(), FiniteDuration(5, TimeUnit.SECONDS))
          if(response.isError) {
            val msg = s"There is an error while writing resources to the FHIR Repository. Repository URL:${sinkSettings.fhirRepoUrl}. Response code:${response.httpStatus.value} Bundle request:${batchRequest.request.resource.get.toJson}"
            logger.error(msg)
            throw FhirMappingException(msg)
          } else {
            logger.debug("{} FHIR resources were written to the FHIR repository successfully.", rowGroup.size)
          }
        })
    }
  }
}
