package io.onfhir.tofhir.engine

import com.typesafe.scalalogging.Logger
import io.onfhir.api.Resource
import io.onfhir.expression.FhirExpressionException
import io.onfhir.tofhir.config.ErrorHandlingType.ErrorHandlingType
import io.onfhir.tofhir.config.{ErrorHandlingType, ToFhirConfig}
import io.onfhir.tofhir.model.{FhirMappingError, FhirMappingErrorCodes, FhirMappingException, FhirMappingResult}
import io.onfhir.util.JsonFormatter._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.json4s.JObject
import org.json4s.jackson.Serialization

import java.sql.Timestamp
import java.time.Instant
import java.util.concurrent.TimeoutException
import scala.concurrent.Await

object MappingTaskExecutor {

  private val logger: Logger = Logger(this.getClass)

  /**
   * Convert input row for mapping to JObject
   *
   * @param row Row to be converted to JObject
   * @return
   */
  def convertRowToJObject(row: Row): JObject = {
    // Row class of Spark has a private method (jsonValue) which transforms a Row into a json4s JObject.
    // We hacked the class to make it accessible and used it to covert a Row into JObject.
    val method = row.getClass.getSuperclass.getInterfaces.apply(0).getMethod("jsonValue")
    method.setAccessible(true)
    method.invoke(row).asInstanceOf[JObject]
  }

  /**
   * Executing the mapping and returning the dataframe for FHIR resources
   *
   * @param spark              Spark session
   * @param df                 DataFrame to be mapped
   * @param fhirMappingService Mapping service for a specific FhirMapping together with contextual data and mapping scripts
   * @return
   */
  def executeMapping(spark: SparkSession, df: DataFrame, fhirMappingService: FhirMappingService, errorHandlingType: ErrorHandlingType): Dataset[FhirMappingResult] = {
    fhirMappingService.sources match {
      case Seq(_) => executeMappingOnSingleSource(spark, df, fhirMappingService, errorHandlingType)
      //Executing on multiple sources
      case _ => throw new NotImplementedError()
    }
  }



  /**
   *
   * @param spark              Spark session
   * @param df                 DataFrame to be mapped
   * @param fhirMappingService Mapping service for a specific FhirMapping together with contextual data and mapping scripts
   * @return
   */
  private def executeMappingOnSingleSource(spark: SparkSession,
                                           df: DataFrame,
                                           fhirMappingService: FhirMappingService,
                                           errorHandlingType: ErrorHandlingType): Dataset[FhirMappingResult] = {
    import spark.implicits._
    val result =
      df
        .flatMap(row => {
          val jo =
            convertRowToJObject(row) //convert the row to JSON object
              .removeField(_._1 == SourceHandler.INPUT_VALIDITY_ERROR) //Remove the extra field appended during validation
              .asInstanceOf[JObject]

          Option(row.getAs[String](SourceHandler.INPUT_VALIDITY_ERROR)) match {
            //If input is valid
            case None => executeMappingOnInput(jo, fhirMappingService, errorHandlingType)
            //If the input is not valid, return the error
            case Some(validationError) =>
              Seq(FhirMappingResult(
                jobId = fhirMappingService.jobId,
                mappingUrl = fhirMappingService.mappingUrl,
                mappingExpr = None, //TODO
                timestamp = Timestamp.from(Instant.now()),
                source = Some(jo.toJson),
                error = Some(FhirMappingError(
                  code = FhirMappingErrorCodes.INVALID_INPUT,
                  description =validationError
                ))
              ))
          }
        })
    result
  }

  /**
   * Execute the mapping on an JSON converted input
   * @param jo                    Input object
   * @param fhirMappingService    Mapping service
   * @param errorHandlingType     Error handling type
   * @return
   */
  private def executeMappingOnInput(jo:JObject,
                                  fhirMappingService: FhirMappingService,
                                  errorHandlingType: ErrorHandlingType):Seq[FhirMappingResult] = {

    val results =
      try {
        val mappedResources = Await.result(fhirMappingService.mapToFhir(jo), ToFhirConfig.mappingTimeout)
        mappedResources.flatMap {
          case (mappingExpr, resources) =>
            resources.map(r =>
              FhirMappingResult(
                jobId = fhirMappingService.jobId,
                mappingUrl = fhirMappingService.mappingUrl,
                mappingExpr = Some(mappingExpr),
                timestamp = Timestamp.from(Instant.now()),
                source = Some(jo.toJson),
                mappedResource = Some(r.toJson)
              )
            )
        }
      } catch {
        // Exception in expression evaluation
        case FhirMappingException(mappingExpr, t:FhirExpressionException) =>
          if (errorHandlingType == ErrorHandlingType.CONTINUE) {
            Seq(
              FhirMappingResult(
                jobId = fhirMappingService.jobId,
                mappingUrl = fhirMappingService.mappingUrl,
                mappingExpr = Some(mappingExpr),
                timestamp = Timestamp.from(Instant.now()),
                source = Some(jo.toJson),
                error = Some(FhirMappingError(
                  code = FhirMappingErrorCodes.MAPPING_ERROR,
                  description = t.msg + t.t.map(_.getMessage).map(" " + _).getOrElse(""),
                  expression = t.expression
                ))
              )
            )
          } else {
            throw FhirMappingException(s"Problem while evaluating the mapping expression $mappingExpr within mapping ${fhirMappingService.mappingUrl}!", t)
          }
        //Other general exceptions
        case e: FhirMappingException =>
          // logger.error(e.getMessage, e)
          if (errorHandlingType == ErrorHandlingType.CONTINUE) {
            Seq(
              FhirMappingResult(
                jobId = fhirMappingService.jobId,
                mappingUrl = fhirMappingService.mappingUrl,
                mappingExpr = None,
                timestamp = Timestamp.from(Instant.now()),
                source = Some(jo.toJson),
                error = Some(FhirMappingError(
                  code = FhirMappingErrorCodes.MAPPING_ERROR,
                  description = e.getMessage
                ))
              )
            )
          } else {
            throw e
          }
        case e: TimeoutException =>
          //logger.error(s"TimeoutException. A single row could not be mapped to FHIR in 5 seconds. The row JObject: ${Serialization.write(jo)}")
          if (errorHandlingType == ErrorHandlingType.CONTINUE) {
            logger.debug("Mapping timeout, continuing the processing of mappings...")
            Seq(
              FhirMappingResult(
                jobId = fhirMappingService.jobId,
                mappingUrl = fhirMappingService.mappingUrl,
                mappingExpr = None,
                timestamp = Timestamp.from(Instant.now()),
                source = Some(jo.toJson),
                error = Some(FhirMappingError(
                  code = FhirMappingErrorCodes.MAPPING_TIMEOUT,
                  description = s"A single row could not be mapped to FHIR in ${ToFhirConfig.mappingTimeout.toString}!"
                ))
              )
            )
          } else {
            logger.debug("Mapping timeout, halting the mapping execution!")
            throw e
          }
      }
    results
  }
}
