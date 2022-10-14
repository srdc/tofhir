package io.onfhir.tofhir.data.write

import com.typesafe.scalalogging.Logger
import io.onfhir.api.client.{FHIRTransactionBatchBundle, FhirBatchTransactionRequestBuilder}
import io.onfhir.tofhir.config.{ErrorHandlingType, ToFhirConfig}
import io.onfhir.tofhir.engine.Execution
import io.onfhir.tofhir.model._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.util.CollectionAccumulator
import org.json4s.jackson.Serialization

import java.util.UUID
import java.util.concurrent.{TimeUnit, TimeoutException}
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext}

/**
 * Class to write the dataset to given FHIR repository
 *
 * @param sinkSettings Settings for the FHIR repository
 */
class FhirRepositoryWriter(sinkSettings: FhirRepositorySinkSettings) extends BaseFhirWriter(sinkSettings) {

  private val logger: Logger = Logger(this.getClass)

  /**
   * Write the data frame of FHIR resources to given FHIR repository
   *
   * @param df
   */
  override def write(spark:SparkSession, df: Dataset[FhirMappingResult], problemsAccumulator:CollectionAccumulator[FhirMappingResult]): Unit = {
    logger.debug("Created FHIR resources will be written to the given FHIR repository URL:{}", sinkSettings.fhirRepoUrl)
    import io.onfhir.util.JsonFormatter._
    df
      .foreachPartition { partition: Iterator[FhirMappingResult] =>
        import Execution.actorSystem
        implicit val ec: ExecutionContext = actorSystem.dispatcher
        val onFhirClient = sinkSettings.createOnFhirClient  // A FhirClient for each partition
        partition
          .grouped(ToFhirConfig.fhirWriterBatchGroupSize)
          .foreach(rowGroup => {
            val resourcesToCreate = rowGroup.map(r => s"urn:uuid:${UUID.randomUUID()}" -> r)
            val resourceMap = resourcesToCreate.toMap
            var batchRequest: FhirBatchTransactionRequestBuilder = onFhirClient.batch()
            resourcesToCreate.foreach(row => {
              val resource = row._2.mappedResource.get.parseJson
              batchRequest = batchRequest.entry(row._1, _.update(resource))
            })
            batchRequest = batchRequest.returnMinimal().asInstanceOf[FhirBatchTransactionRequestBuilder]
            logger.debug("Batch Update request will be sent to the FHIR repository for {} resources.", rowGroup.size)
            var responseBundle: FHIRTransactionBatchBundle = null
            try {
              responseBundle = Await.result(batchRequest.executeAndReturnBundle(), FiniteDuration(5, TimeUnit.SECONDS))
            } catch {
              case tout: TimeoutException =>
                val msg = s"FHIR repository at url ${sinkSettings.fhirRepoUrl} timeout for batch interaction while writing the resources!"
                if (sinkSettings.errorHandling.isEmpty || sinkSettings.errorHandling.get == ErrorHandlingType.HALT) {
                  logger.error(msg, tout)
                  throw FhirMappingException(msg, tout)
                } else {
                  resourcesToCreate
                    .map(_._2)
                    .map(mr =>
                      mr.copy(error = Some(FhirMappingError( //Set the error
                        code = FhirMappingErrorCodes.FHIR_API_TIMEOUT,
                        description = msg
                      )))
                    )
                    .foreach(failedResult =>
                        problemsAccumulator.add(failedResult)
                    )
                }
              case e: Throwable =>
                val msg = "!!!There is an error while writing resources to the FHIR Repository."
                if (sinkSettings.errorHandling.isEmpty || sinkSettings.errorHandling.get == ErrorHandlingType.HALT) {
                  logger.error(msg, e)
                  throw FhirMappingException(msg, e)
                } else {
                  logger.error(msg, e)
                  resourceMap.values
                    .map(mr =>
                      mr.copy(error = Some(FhirMappingError( //Set the error
                        code = FhirMappingErrorCodes.SERVICE_PROBLEM,
                        description = msg + " " + e.getMessage
                      )))
                    ).foreach(failedResult =>
                      problemsAccumulator.add(failedResult)
                    )
                }
            }
            if(responseBundle != null) {
              //Check if there is any error in one of the requests
              if (responseBundle.hasAnyError()) {
                val msg =
                  s"!!!There is an error while writing resources to the FHIR Repository.\n" +
                    s"\tRepository URL: ${sinkSettings.fhirRepoUrl}\n" +
                    s"\tBundle requests: ${batchRequest.request.childRequests.map(_.requestUri).mkString(",")}\n" +
                    s"\tBundle response: ${responseBundle.bundle.toJson}"

                responseBundle
                  .responses
                  .filter(_._2.isError)
                  .map(response =>
                    resourceMap(response._1.get) //Find the mapping result
                      .copy(error = Some(FhirMappingError( //Set the error
                        code = FhirMappingErrorCodes.INVALID_RESOURCE,
                        description = "Resource is not a valid FHIR resource or conforming to the indicated profiles!",
                        expression = Some(Serialization.write(response._2.outcomeIssues))
                      )))
                  ).foreach(failedResult =>
                  problemsAccumulator.add(failedResult)
                )

                logger.error(msg)
                if (sinkSettings.errorHandling.isEmpty || sinkSettings.errorHandling.get == ErrorHandlingType.HALT) {
                  throw FhirMappingException(msg)
                }
              } else {
                logger.debug("{} FHIR resources were written to the FHIR repository successfully.", rowGroup.size)
              }
            }
          })
      }
  }
}
