package io.onfhir.tofhir.engine

import com.typesafe.scalalogging.Logger
import io.onfhir.api.client.FHIRTransactionBatchBundle
import io.onfhir.client.OnFhirNetworkClient
import io.onfhir.client.OnFhirNetworkClient.system.dispatcher
import io.onfhir.tofhir.model.{FhirMappingException, FhirRepositorySinkSettings}
import org.apache.spark.sql.Dataset

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

  val BATCH_GROUP_SIZE: Int = 10

  /**
   * Write the data frame of FHIR resources to given FHIR repository
   *
   * @param df
   */
  override def write(df:  Dataset[String]): Unit = {
    logger.debug("Created FHIR resources will be written to the given FHIR repository URL:{}", sinkSettings.fhirRepoUrl)
    import io.onfhir.util.JsonFormatter._
    df
      .foreachPartition { partition: Iterator[String] =>
        val onFhirClient = OnFhirNetworkClient.apply(sinkSettings.fhirRepoUrl) // A FhirClient for each partition
        partition
          .grouped(BATCH_GROUP_SIZE)
          .foreach(rowGroup => {
            var batchRequest = onFhirClient.batch()
            rowGroup.foreach(row => {
              val resource = row.parseJson
              batchRequest = batchRequest.entry(_.update(resource).returnMinimal())
            })
            logger.debug("Batch Update request will be sent to the FHIR repository for {} resources.", rowGroup.size)
            var responseBundle:FHIRTransactionBatchBundle = null
              try {
                responseBundle = Await.result(batchRequest.executeAndReturnBundle(), FiniteDuration(5, TimeUnit.SECONDS))
              } catch {
                case e:Throwable => throw FhirMappingException("!!!There is an error while writing resources to the FHIR Repository.", e)
              }
              //Check if there is any error in one of the requests
              if (responseBundle.hasAnyError()) {
                val msg =
                  s"!!!There is an error while writing resources to the FHIR Repository.\n" +
                    s"Repository URL: ${sinkSettings.fhirRepoUrl}\n" +
                    s"Bundle requests: ${batchRequest.request.childRequests.map(_.requestUri).mkString(",")}\n" +
                    s"Bundle response: ${responseBundle.bundle.toJson}"
                logger.error(msg)
                throw FhirMappingException(msg)
              } else {
                logger.debug("{} FHIR resources were written to the FHIR repository successfully.", rowGroup.size)
              }
      })
    }
  }
}
