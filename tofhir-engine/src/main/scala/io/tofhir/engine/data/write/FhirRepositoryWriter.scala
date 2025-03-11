package io.tofhir.engine.data.write

import akka.http.scaladsl.model.Uri
import com.typesafe.scalalogging.Logger
import io.onfhir.api.FHIR_INTERACTIONS
import io.onfhir.api.client.{FHIRTransactionBatchBundle, FhirBatchTransactionRequestBuilder, FhirClientException}
import io.onfhir.api.model.OutcomeIssue
import io.onfhir.api.util.FHIRUtil
import io.onfhir.client.OnFhirNetworkClient
import io.onfhir.definitions.common.model.Json4sSupport.formats
import io.tofhir.engine.Execution
import io.tofhir.engine.config.ToFhirConfig
import io.tofhir.engine.model._
import io.tofhir.engine.model.exception.InvalidFhirRepositoryUrlException
import org.apache.hadoop.shaded.org.apache.http.HttpStatus
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.util.CollectionAccumulator
import org.json4s.jackson.{JsonMethods, Serialization}

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
  override def write(spark: SparkSession, df: Dataset[FhirMappingResult], problemsAccumulator: CollectionAccumulator[FhirMappingResult]): Unit = {
    logger.debug("Created FHIR resources will be written to the given FHIR repository URL:{}", sinkSettings.fhirRepoUrl)
    df
      .foreachPartition { partition: Iterator[FhirMappingResult] =>
        import Execution.actorSystem
        implicit val ec: ExecutionContext = actorSystem.dispatcher
        val onFhirClient = sinkSettings.createOnFhirClient // A FhirClient for each partition
        partition
          .grouped(ToFhirConfig.engineConfig.fhirWriterBatchGroupSize)
          .foreach(rowGroup => {
            // Construct a FHIR batch operation from each entry
            val batchRequest = prepareBatchRequest(rowGroup, onFhirClient)
            logger.debug("Batch Update request will be sent to the FHIR repository for {} resources.", rowGroup.size)

            executeFhirBatch(batchRequest, rowGroup, problemsAccumulator)
              .foreach(responseBundle =>
                checkResults(rowGroup, responseBundle, batchRequest, problemsAccumulator, onFhirClient)
              )
          })
      }
  }

  /**
   * Validates the current FHIR writer. It checks whether the provided FHIR repository URL is valid by attempting to
   * retrieve the capability statement from the server. If the server does not respond with a successful status code
   * within the specified timeout period, or if the response indicates an error, an InvalidFhirRepositoryUrlException is
   * thrown.
   *
   * @throws InvalidFhirRepositoryUrlException if the validation fails due to an invalid or unreachable FHIR repository URL.
   */
  override def validate(): Unit = {
    import Execution.actorSystem
    implicit val ec: ExecutionContext = actorSystem.dispatcher
    val onFhirClient = sinkSettings.createOnFhirClient
    try {
      Await.result(onFhirClient.capabilities().execute()
        .map(response => {
          if (!response.httpStatus.isSuccess()) {
            throw InvalidFhirRepositoryUrlException(s"Failed to retrieve capability statement for the FHIR Server at '${onFhirClient.getBaseUrl()}' which returns ${response.httpStatus}. Please make sure that the URL is correct.")
          }
        }), FiniteDuration(10, TimeUnit.SECONDS))
    } catch {
      case t: InvalidFhirRepositoryUrlException =>
        throw t
      case te: TimeoutException =>
        throw InvalidFhirRepositoryUrlException(s"Failed to retrieve capability statement for the FHIR Server at '${onFhirClient.getBaseUrl()}' in 10 seconds. Please make sure that the URL is correct.", te)
      case e: Throwable =>
        throw InvalidFhirRepositoryUrlException(s"Failed to retrieve capability statement for the FHIR Server at '${onFhirClient.getBaseUrl()}'. Please make sure that the URL is correct.", e)
    }
  }

  /**
   * Prepare the FHIR batch request from mapping results
   *
   * @param mappingResults Mapping results for this FHIR batch
   * @param onFhirClient   OnFhirClient
   * @return
   */
  private def prepareBatchRequest(mappingResults: Seq[FhirMappingResult], onFhirClient: OnFhirNetworkClient): FhirBatchTransactionRequestBuilder = {
    import io.onfhir.util.JsonFormatter._
    // Construct a FHIR batch operation from each entry
    var batchRequest: FhirBatchTransactionRequestBuilder = onFhirClient.batch()
    mappingResults
      .foreach {
        mappingResult =>
          // create an uuid for the batch request
          val uuid = s"urn:uuid:${UUID.randomUUID()}"
          mappingResult
            .mappedFhirResource.get
            .fhirInteraction
            .getOrElse(FhirInteraction(FHIR_INTERACTIONS.UPDATE)) match {
            //FHIR Update (update or create by update)
            case FhirInteraction(FHIR_INTERACTIONS.UPDATE, _, None) =>
              val resource = mappingResult.mappedFhirResource.get.mappedResource.get.parseJson
              batchRequest = batchRequest.entry(uuid, _.update(resource))
            //FHIR Conditional update
            case FhirInteraction(FHIR_INTERACTIONS.UPDATE, _, Some(condition)) =>
              val resource = mappingResult.mappedFhirResource.get.mappedResource.get.parseJson
              val searchParams = Uri(condition).query().toMultiMap
              batchRequest = batchRequest.entry(uuid, _.update(resource).where(searchParams))
            //FHIR Create
            case FhirInteraction(FHIR_INTERACTIONS.CREATE, _, None) =>
              val resource = mappingResult.mappedFhirResource.get.mappedResource.get.parseJson
              batchRequest = batchRequest.entry(uuid, _.create(resource))
            // FHIR Conditional create, if not exists
            case FhirInteraction(FHIR_INTERACTIONS.CREATE, _, Some(condition)) =>
              val resource = mappingResult.mappedFhirResource.get.mappedResource.get.parseJson
              val searchParams = Uri(condition).query().toMultiMap
              batchRequest = batchRequest.entry(uuid, _.create(resource).where(searchParams))
            //FHIR Patch
            case FhirInteraction(FHIR_INTERACTIONS.PATCH, Some(rtypeAndId), None) =>
              val patchContent = JsonMethods.parse(mappingResult.mappedFhirResource.get.mappedResource.get)
              val (_, rtype, rid, _) = FHIRUtil.parseReferenceValue(rtypeAndId)
              batchRequest =
                batchRequest
                  .entry(uuid, _.patch(rtype, rid)
                    .patchContent(patchContent))
            //Conditional patch
            case FhirInteraction(FHIR_INTERACTIONS.PATCH, Some(rtype), Some(condition)) =>
              val patchContent = JsonMethods.parse(mappingResult.mappedFhirResource.get.mappedResource.get)
              val searchParams = Uri(condition).query().toMultiMap

              batchRequest =
                batchRequest
                  .entry(uuid, _.patch(rtype).where(searchParams).patchContent(patchContent))
          }
      }
    // add 'return=minimal' header if asked
    if (sinkSettings.returnMinimal)
      batchRequest = batchRequest.returnMinimal().asInstanceOf[FhirBatchTransactionRequestBuilder]
    batchRequest
  }

  /**
   * Execute the FHIR batch to persist the mapped information
   *
   * @param batchRequest        Batch request
   * @param mappingResults      Mapping results
   * @param problemsAccumulator Spark accumulator for problems
   * @param ec
   * @return
   */
  private def executeFhirBatch(batchRequest: FhirBatchTransactionRequestBuilder,
                               mappingResults: Seq[FhirMappingResult],
                               problemsAccumulator: CollectionAccumulator[FhirMappingResult]
                              )(implicit ec: ExecutionContext): Option[FHIRTransactionBatchBundle] = {
    try {
      Some(Await.result(batchRequest.executeAndReturnBundle(), FiniteDuration(20, TimeUnit.SECONDS)))
    } catch {
      case fce: FhirClientException =>
        if (fce.serverResponse.isDefined && fce.serverResponse.get.outcomeIssues.nonEmpty) {
          // Firely returns HTTP 400 which causes a FhirClientException in case any of the records in the batch request raises an error.
          // This is a contradiction with the FHIR standard. The standard says:
          //  "When processing the batch, the HTTP response code is 200 OK
          //    if the batch was processed correctly, regardless of the success of the operations within the Batch."
          // For this reason, we will handle a kind of "checkResults" (see the function below) at this point for Firely.

          // find the validation errors of mappings
          val validationErrorsOfMappings = groupOutcomeIssuesByEntryIndex(fce.serverResponse.get.outcomeIssues)
          mappingResults
            .zipWithIndex.map {
            case (element, index) =>
              element.copy(
                error = Some(
                  FhirMappingError(
                    code = FhirMappingErrorCodes.INVALID_RESOURCE,
                    description = "Resource is not a valid FHIR resource or does not conform to the indicated profiles. Additionally, this resource might be skipped due to duplicate entries in the batch/transaction!",
                    expression = Some(Serialization.write(validationErrorsOfMappings.getOrElse(index, None)))
                  )
                )
              )
          }.foreach(failedResult => problemsAccumulator.add(failedResult))
          None
        } else {
          val msg = s"FHIR repository at url ${sinkSettings.fhirRepoUrl} returned an unidentified error while writing the resources!"
          mappingResults
            .map(mr =>
              mr.copy(error = Some(FhirMappingError( //Set the error
                code = FhirMappingErrorCodes.SERVICE_PROBLEM,
                description = msg + Option(fce.getMessage).filter(_.nonEmpty).map(" " + _).getOrElse("")
              )))
            ).foreach(failedResult => problemsAccumulator.add(failedResult))
          None
        }
      case tout: TimeoutException =>
        val msg = s"FHIR repository at url ${sinkSettings.fhirRepoUrl} timeout for batch interaction while writing the resources!"
        mappingResults
          .map(mr =>
            mr.copy(error = Some(FhirMappingError( //Set the error
              code = FhirMappingErrorCodes.FHIR_API_TIMEOUT,
              description = msg
            )))
          ).foreach(failedResult => problemsAccumulator.add(failedResult))
        None
      case e: Throwable =>
        val msg = "UNEXPECTED!!! There is an unidentified error while writing resources to the FHIR Repository."
        mappingResults
          .map(mr =>
            mr.copy(error = Some(FhirMappingError( //Set the error
              code = FhirMappingErrorCodes.SERVICE_PROBLEM,
              description = msg + " " + e.getMessage
            )))
          ).foreach(failedResult => problemsAccumulator.add(failedResult))
        None
    }
  }

  /**
   * Groups a sequence of OutcomeIssues by their entry index in the resource bundle.
   *
   * @param outcomeIssues The sequence of OutcomeIssues to be grouped.
   * @return A map where the keys are resource entry indices, and the values are sequences of OutcomeIssues associated with each index.
   */
  private def groupOutcomeIssuesByEntryIndex(outcomeIssues: Seq[OutcomeIssue]): Map[Int, Seq[OutcomeIssue]] = {
    outcomeIssues.groupBy { issue =>
      if (issue.expression.isEmpty) {
        // If Firely does not return an expression for the OutcomeIssue, I cannot find for which FhirMappingResult this issue is raised!!!
        logger.error(s"Firely did not return an expression indicating the location of the OutcomeIssue: ${Serialization.write(issue)}")
        None
      } else {
        if (issue.expression.size > 1) {
          logger.warn(s"There are more than one expression describing the location of the OutcomeIssue. I will continue with the first expression: ${issue.expression.head}")
        }
        val resourceEntryIndex = // Find the index of the resource so that I can find it in the mappingResults.
          "^Bundle\\.entry\\[(\\d+)\\]".r
            .findFirstMatchIn(issue.expression.head) match {
            case Some(matched) => Some(matched.group(1).toInt)
            case None => None
          }
        if (resourceEntryIndex.isEmpty) {
          logger.error(s"Entry index cannot be extracted from the expression given in the OutcomeIssue: ${Serialization.write(issue)}")
        }
        resourceEntryIndex
      }
    }.collect {
      case (Some(index), issues) => index -> issues
    }
  }

  /**
   * Check the results of persistence interactions and perform error handling
   *
   * @param mappingResults      Mapped results
   * @param responseBundle      FHIR batch response bundle
   * @param batchRequest        FHIR batch request
   * @param problemsAccumulator Spark accumulator for errors
   * @param onFhirClient        Client for FHIR API
   * @param retry               The number of retry for persistence
   */
  private def checkResults(mappingResults: Seq[FhirMappingResult],
                           responseBundle: FHIRTransactionBatchBundle,
                           batchRequest: FhirBatchTransactionRequestBuilder,
                           problemsAccumulator: CollectionAccumulator[FhirMappingResult],
                           onFhirClient: OnFhirNetworkClient,
                           retry: Int = 1
                          )(implicit ec: ExecutionContext): Unit = {
    //Find real errors
    val transientErrors = responseBundle.getUUIDsOfTransientErrors()
    if (hasNonTransientErrors(mappingResults, responseBundle) || retry == 4) {
      val msg =
        s"!!!There is an error while writing resources to the FHIR Repository.\n" +
          s"\tRepository URL: ${sinkSettings.fhirRepoUrl}\n" +
          s"\tBundle requests: ${batchRequest.request.childRequests.map(_.requestUri).mkString(",")}\n" +
          s"\tBundle response: ${Serialization.write(responseBundle.bundle)}"

      // Previously, we assigned a UUID to each batch request and matched the corresponding mapping result
      // using the UUID returned in the FHIR response for that batch request. While this approach worked
      // with onFHIR, it is incompatible with the HAPI FHIR Server, as the server does not return the UUID
      // in the batch response. Consequently, this code is no longer in use.

      //      responseBundle
      //        .responses
      //        .filter(_._2.isError)
      //        .map(response =>
      //          mappingResultMap(response._1.get) //Find the mapping result
      //            .copy(error = Some(FhirMappingError( //Set the error
      //              code = FhirMappingErrorCodes.INVALID_RESOURCE,
      //              description = "Resource is not a valid FHIR resource or conforming to the indicated profiles!",
      //              expression = Some(Serialization.write(response._2.outcomeIssues))
      //            )))
      //        ).foreach(failedResult => problemsAccumulator.add(failedResult))

      responseBundle
        .responses
        .zipWithIndex // Add indexes to each response
        .filter(_._1._2.isError) // Filter based on the error condition
        .map { case ((response, index)) =>
          mappingResults(index) // Use the index to find the mapping result
            .copy(error = Some(FhirMappingError( // Set the error
              code = FhirMappingErrorCodes.INVALID_RESOURCE,
              description = "Resource is not a valid FHIR resource or conforming to the indicated profiles!",
              expression = Some(Serialization.write(response._2.outcomeIssues))
            )))
        }
        .foreach(failedResult => problemsAccumulator.add(failedResult))
      logger.error(msg)
    } else if (transientErrors.nonEmpty) {
      //Otherwise (having 409 Conflicts), retry the failed ones
      retryRequestsWithTransientError(mappingResults, responseBundle, problemsAccumulator, onFhirClient, retry)
    } else {
      logger.debug("{} FHIR resources were written to the FHIR repository successfully.", mappingResults.size)
    }
  }

  /**
   * Checks if there are any non-transient errors in the FHIR response bundle.
   *
   * A non-transient error is an error that is not expected to resolve on retry.
   *
   * @param mappingResults   A sequence of FhirMappingResult objects.
   * @param responseBundle   A FHIRTransactionBatchBundle containing the responses of a batch operation.
   * @return                 `true` if there are any non-transient errors, `false` otherwise.
   */
  private def hasNonTransientErrors(mappingResults: Seq[FhirMappingResult], responseBundle: FHIRTransactionBatchBundle): Boolean = {
    responseBundle
      .responses
      .zipWithIndex
      .exists { case (r,i) =>
        r._2.isNonTransientError && //If this is a non-transient error, except the Conditional patch not found case, which we skip the update
          !(
            r._2.httpStatus.intValue() == HttpStatus.SC_NOT_FOUND &&
              mappingResults(i).mappedFhirResource.get.fhirInteraction.exists(fint => fint.`type` == FHIR_INTERACTIONS.PATCH && fint.condition.nonEmpty)
            )
      }
  }

  /**
   * Retry the FHIR requests with transient errors
   *
   * @param mappingResults      Mapped results
   * @param responseBundle      FHIR batch response bundle
   * @param problemsAccumulator Spark accumulator for errors
   * @param onFhirClient        Client for FHIR API
   * @param retry               The number of retry for persistence
   * @param ec
   */
  private def retryRequestsWithTransientError(mappingResults: Seq[FhirMappingResult],
                                              responseBundle: FHIRTransactionBatchBundle,
                                              problemsAccumulator: CollectionAccumulator[FhirMappingResult],
                                              onFhirClient: OnFhirNetworkClient,
                                              retry: Int = 1
                                             )(implicit ec: ExecutionContext): Unit = {
    // find the indices of responses with the transient errors
    val transientIndices: Seq[Int] = responseBundle.responses
      .zipWithIndex.collect {
        case ((_, response), index) if response.isTransientError => index
    }
    // find the corresponding mapping results
    val mappingResultsNotPersisted: Seq[FhirMappingResult] = transientIndices.map(mappingResults)
    logger.warn(s"Some mapped FHIR content (${mappingResultsNotPersisted.size} entries) is not persisted due to conflicts (409 conflict), retrying (retry #$retry) for them...")
    Thread.sleep(50)
    val batchRequest = prepareBatchRequest(mappingResultsNotPersisted, onFhirClient)
    executeFhirBatch(batchRequest, mappingResultsNotPersisted, problemsAccumulator)
      .foreach(retryResponseBundle =>
        checkResults(mappingResultsNotPersisted, retryResponseBundle, batchRequest, problemsAccumulator, onFhirClient, retry + 1)
      )
  }
}
