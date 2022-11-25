package io.tofhir.engine.data.write

import akka.http.scaladsl.model.Uri
import com.typesafe.scalalogging.Logger
import io.onfhir.api.FHIR_INTERACTIONS
import io.onfhir.api.client.{FHIRTransactionBatchBundle, FhirBatchTransactionRequestBuilder}
import io.onfhir.api.util.FHIRUtil
import io.onfhir.client.OnFhirNetworkClient
import io.onfhir.util.JsonFormatter
import io.tofhir.engine.config.{ErrorHandlingType, ToFhirConfig}
import io.tofhir.engine.mapping.Execution
import io.tofhir.engine.model.{FhirInteraction, FhirMappingError, FhirMappingErrorCodes, FhirMappingException, FhirMappingResult, FhirRepositorySinkSettings}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.util.CollectionAccumulator
import org.json4s.JArray
import org.json4s.jackson.{JsonMethods, Serialization}

import java.util.UUID
import java.util.concurrent.{TimeUnit, TimeoutException}
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext}
import io.onfhir.util.JsonFormatter._
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

    df
      .foreachPartition { partition: Iterator[FhirMappingResult] =>
        import Execution.actorSystem
        implicit val ec: ExecutionContext = actorSystem.dispatcher
        val onFhirClient = sinkSettings.createOnFhirClient  // A FhirClient for each partition
        partition
          .grouped(ToFhirConfig.fhirWriterBatchGroupSize)
          .foreach(rowGroup => {
            //Create a UUID for each entry
            val resourcesToCreate = rowGroup.map(r => s"urn:uuid:${UUID.randomUUID()}" -> r)
            val resourceMap = resourcesToCreate.toMap
            // Construct a FHIR batch operation from each entry
            val batchRequest = prepareBatchRequest(resourcesToCreate, onFhirClient)
            logger.debug("Batch Update request will be sent to the FHIR repository for {} resources.", rowGroup.size)

            executeFhirBatch(batchRequest, resourcesToCreate, problemsAccumulator)
              .foreach(responseBundle =>
                checkResults(resourceMap, responseBundle, batchRequest, problemsAccumulator, onFhirClient)
              )
            })
      }
  }

  /**
   * Prepare the batch request from mapping results
   * @param mappingResults  Mapping results for this batch
   * @param onFhirClient    OnFhirClient
   * @return
   */
  private def prepareBatchRequest(mappingResults: Seq[(String, FhirMappingResult)], onFhirClient: OnFhirNetworkClient): FhirBatchTransactionRequestBuilder = {
    import io.onfhir.util.JsonFormatter._
    // Construct a FHIR batch operation from each entry
    var batchRequest: FhirBatchTransactionRequestBuilder = onFhirClient.batch()
    mappingResults
      .foreach {
        case (uuid, mappingResult) =>
          mappingResult
            .fhirInteraction
            .getOrElse(FhirInteraction(FHIR_INTERACTIONS.UPDATE)) match {
            //FHIR Update (update or create by update)
            case FhirInteraction(FHIR_INTERACTIONS.UPDATE, _, None) =>
              val resource = mappingResult.mappedResource.get.parseJson
              batchRequest = batchRequest.entry(uuid, _.update(resource))
            //FHIR Conditional update
            case FhirInteraction(FHIR_INTERACTIONS.UPDATE, _, Some(condition)) =>
              val resource = mappingResult.mappedResource.get.parseJson
              val searchParams = Uri(condition).query().toMultiMap
              batchRequest = batchRequest.entry(uuid, _.update(resource).where(searchParams))
            //FHIR Create
            case FhirInteraction(FHIR_INTERACTIONS.CREATE, _, None) =>
              val resource = mappingResult.mappedResource.get.parseJson
              batchRequest = batchRequest.entry(uuid, _.create(resource))
            // FHIR Conditional create, if not exists
            case FhirInteraction(FHIR_INTERACTIONS.CREATE, _, Some(condition)) =>
              val resource = mappingResult.mappedResource.get.parseJson
              val searchParams = Uri(condition).query().toMultiMap
              batchRequest = batchRequest.entry(uuid, _.create(resource).where(searchParams))
            //FHIR Patch
            case FhirInteraction(FHIR_INTERACTIONS.PATCH, Some(rtypeAndId), None) =>
              val patchContent = JsonMethods.parse(mappingResult.mappedResource.get)
              val (_, rtype, rid, _) = FHIRUtil.parseReferenceValue(rtypeAndId)
              batchRequest =
                batchRequest
                  .entry(uuid, _.patch(rtype, rid)
                    .patchContent(patchContent))
            //Conditional patch
            case FhirInteraction(FHIR_INTERACTIONS.PATCH, Some(rtype), Some(condition)) =>
              val patchContent = JsonMethods.parse(mappingResult.mappedResource.get)
              val searchParams = Uri(condition).query().toMultiMap

              batchRequest =
                batchRequest
                  .entry(uuid, _.patch(rtype).where(searchParams).patchContent(patchContent))
          }
      }
    batchRequest = batchRequest.returnMinimal().asInstanceOf[FhirBatchTransactionRequestBuilder]
    batchRequest
  }

  /**
   * Execute the FHIR batch to persist the mapped information
   * @param batchRequest          Batch request
   * @param mappingResults        Mapping results
   * @param problemsAccumulator   Spark accumulator for problems
   * @param ec
   * @return
   */
  private def executeFhirBatch(batchRequest:FhirBatchTransactionRequestBuilder,
                           mappingResults: Seq[(String, FhirMappingResult)],
                           problemsAccumulator:CollectionAccumulator[FhirMappingResult]
                          )(implicit ec:ExecutionContext):Option[FHIRTransactionBatchBundle] = {
    try {
      Some(Await.result(batchRequest.executeAndReturnBundle(), FiniteDuration(5, TimeUnit.SECONDS)))
    } catch {
      case tout: TimeoutException =>
        val msg = s"FHIR repository at url ${sinkSettings.fhirRepoUrl} timeout for batch interaction while writing the resources!"
        if (sinkSettings.errorHandling.isEmpty || sinkSettings.errorHandling.get == ErrorHandlingType.HALT) {
          logger.error(msg, tout)
          throw FhirMappingException(msg, tout)
        } else {
          mappingResults
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
          None
        }
      case e: Throwable =>
        val msg = "!!!There is an error while writing resources to the FHIR Repository."
        if (sinkSettings.errorHandling.isEmpty || sinkSettings.errorHandling.get == ErrorHandlingType.HALT) {
          logger.error(msg, e)
          throw FhirMappingException(msg, e)
        } else {
          logger.error(msg, e)
          mappingResults
            .map(_._2)
            .map(mr =>
              mr.copy(error = Some(FhirMappingError( //Set the error
                code = FhirMappingErrorCodes.SERVICE_PROBLEM,
                description = msg + " " + e.getMessage
              )))
            ).foreach(failedResult =>
              problemsAccumulator.add(failedResult)
            )
          None
        }
    }
  }

  /**
   * Check the results of persistence interactions and perform error handling
   *
   * @param mappingResultMap    Mapped results
   * @param responseBundle      FHIR batch response bundle
   * @param batchRequest        FHIR batch request
   * @param problemsAccumulator Spark accumulator for errors
   * @param onFhirClient        Client for FHIR API
   * @param retry               The number of retry for persistence
   */
  private def checkResults(mappingResultMap: Map[String, FhirMappingResult],
                           responseBundle: FHIRTransactionBatchBundle,
                           batchRequest: FhirBatchTransactionRequestBuilder,
                           problemsAccumulator: CollectionAccumulator[FhirMappingResult],
                           onFhirClient: OnFhirNetworkClient,
                           retry: Int = 1
                          )(implicit ec: ExecutionContext): Unit = {
    //Check if there is any error in one of the requests
    if (responseBundle.hasAnyError()) {
      //If errors are non-transient errors, log them
      if (responseBundle.hasAnyNonTransientError() || retry == 3) {
        val msg =
          s"!!!There is an error while writing resources to the FHIR Repository.\n" +
            s"\tRepository URL: ${sinkSettings.fhirRepoUrl}\n" +
            s"\tBundle requests: ${batchRequest.request.childRequests.map(_.requestUri).mkString(",")}\n" +
            s"\tBundle response: ${responseBundle.bundle.toJson}"

        responseBundle
          .responses
          .filter(_._2.isError)
          .map(response =>
            mappingResultMap(response._1.get) //Find the mapping result
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
      }
      //Otherwise (having 409 Conflicts), retry the failed ones
      else {
        retryRequestsWithTransientError(mappingResultMap, responseBundle, problemsAccumulator, onFhirClient, retry)
      }
    } else {
      logger.debug("{} FHIR resources were written to the FHIR repository successfully.", mappingResultMap.size)
    }
  }

  /**
   * Retry the FHIR requests with transient errors
   *
   * @param mappingResultMap    Mapped results
   * @param responseBundle      FHIR batch response bundle
   * @param batchRequest        FHIR batch request
   * @param problemsAccumulator Spark accumulator for errors
   * @param onFhirClient        Client for FHIR API
   * @param retry               The number of retry for persistence
   * @param ec
   */
  private def retryRequestsWithTransientError( mappingResultMap: Map[String, FhirMappingResult],
                                               responseBundle: FHIRTransactionBatchBundle,
                                              problemsAccumulator: CollectionAccumulator[FhirMappingResult],
                                               onFhirClient: OnFhirNetworkClient,
                                               retry:Int = 1
                                             )(implicit ec:ExecutionContext):Unit= {
    val entryIdsForProblematicRequests = responseBundle.getUUIDsOfTransientErrors().toSet
    val mappingResultsNotPersisted = mappingResultMap.filter(r => entryIdsForProblematicRequests.contains(r._1))
    logger.warn(s"Some mapped FHIR content (${mappingResultsNotPersisted.size} entries) is not persisted due to conflicts (409 conflict), retrying (retry #$retry) for them...")
    Thread.sleep(50)
    val batchRequest = prepareBatchRequest(mappingResultsNotPersisted.toSeq, onFhirClient)
    executeFhirBatch(batchRequest, mappingResultsNotPersisted.toSeq, problemsAccumulator)
      .foreach(retryResponseBundle =>
        checkResults(mappingResultsNotPersisted, retryResponseBundle,batchRequest,problemsAccumulator,onFhirClient, retry + 1)
      )
  }
}