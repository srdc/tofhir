package io.tofhir.engine.mapping

import com.typesafe.scalalogging.Logger
import io.onfhir.expression.FhirExpressionException
import io.onfhir.util.JsonFormatter._
import io.tofhir.common.util.ExceptionUtil
import io.tofhir.engine.config.ErrorHandlingType.ErrorHandlingType
import io.tofhir.engine.config.{ErrorHandlingType, ToFhirConfig}
import io.tofhir.engine.data.read.SourceHandler
import io.tofhir.engine.model.{FhirMappingError, FhirMappingErrorCodes, FhirMappingException, FhirMappingResult}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.json4s.JsonAST.{JArray, JObject, JValue}

import java.sql.Timestamp
import java.time.Instant
import java.util.concurrent.TimeoutException
import scala.concurrent.Await
import scala.jdk.CollectionConverters.ListHasAsScala

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
   * @param executionId        Id of FhirMappingJobExecution object
   * @return
   */
  def executeMapping(spark: SparkSession, df: DataFrame, fhirMappingService: FhirMappingService, errorHandlingType: ErrorHandlingType, executionId: Option[String] = None): Dataset[FhirMappingResult] = {
    fhirMappingService.sources match {
      case Seq(_) => executeMappingOnSingleSource(spark, df, fhirMappingService, errorHandlingType, executionId)
      //Executing on multiple sources
      case oth => executeMappingOnMultipleSources(spark, df, fhirMappingService, oth, errorHandlingType, executionId)
    }
  }

  /**
   *
   * @param spark              Spark session
   * @param df                 DataFrame to be mapped
   * @param fhirMappingService Mapping service for a specific FhirMapping together with contextual data and mapping scripts
   * @param executionId        Id of FhirMappingJobExecution object
   * @return
   */
  private def executeMappingOnSingleSource(spark: SparkSession,
                                           df: DataFrame,
                                           fhirMappingService: FhirMappingService,
                                           errorHandlingType: ErrorHandlingType,
                                           executionId: Option[String] = None): Dataset[FhirMappingResult] = {
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
            case None => executeMappingOnInput(jo, Map.empty[String, JValue], fhirMappingService, errorHandlingType, executionId)
            //If the input is not valid, return the error
            case Some(validationError) =>
              Seq(FhirMappingResult(
                jobId = fhirMappingService.jobId,
                mappingUrl = fhirMappingService.mappingUrl,
                mappingExpr = None,
                timestamp = Timestamp.from(Instant.now()),
                source = Some(jo.toJson),
                error = Some(FhirMappingError(
                  code = FhirMappingErrorCodes.INVALID_INPUT,
                  description = validationError
                )),
                executionId = executionId
              ))
          }
        })
    result
  }

  private def executeMappingOnMultipleSources(spark: SparkSession,
                                              df: DataFrame,
                                              fhirMappingService: FhirMappingService,
                                              sources: Seq[String],
                                              errorHandlingType: ErrorHandlingType,
                                              executionId: Option[String] = None): Dataset[FhirMappingResult] = {
    import spark.implicits._
    val result =
      df
        .flatMap(row => {
          val otherSourceRows =
            sources
              .tail
              .map(s => s -> Option(row.getList[Row](row.schema.fieldIndex(s"__$s"))).map(_.asScala.toList).getOrElse(List.empty))

          val mainSource = row.getStruct(row.schema.fieldIndex(s"__${sources.head}"))

          //Check if there is any validation error
          val validationErrors =
            Option(mainSource.getAs[String](SourceHandler.INPUT_VALIDITY_ERROR)).toSeq ++
              otherSourceRows.flatMap(rows => rows._2.flatMap(r => Option(r.getAs[String](SourceHandler.INPUT_VALIDITY_ERROR))))

          val jo =
            convertRowToJObject(mainSource) //convert the row to JSON object
              .removeField(f => f._1.startsWith("__")) //Remove the extra field appended during validation and other source objects
              .asInstanceOf[JObject]
          //Parse data coming from other sources as context parameters
          val otherObjectMap: Map[String, JValue] =
            otherSourceRows
              .flatMap {
                case (alias, rows) =>
                  rows
                    .map(r => convertRowToJObject(r).removeField(f => f._1.startsWith("__"))) match {
                    case Nil => None
                    case Seq(o) => Some(alias -> o)
                    case oth => Some(alias -> JArray(oth))
                  }
              }
              .toMap

          validationErrors match {
            //If input is valid
            case Nil => executeMappingOnInput(jo, otherObjectMap, fhirMappingService, errorHandlingType, executionId)
            //If the input is not valid, return the error
            case _ =>
              Seq(FhirMappingResult(
                jobId = fhirMappingService.jobId,
                mappingUrl = fhirMappingService.mappingUrl,
                mappingExpr = None,
                timestamp = Timestamp.from(Instant.now()),
                source = Some(jo.toJson),
                error = Some(FhirMappingError(
                  code = FhirMappingErrorCodes.INVALID_INPUT,
                  description = validationErrors.mkString("\n")
                )),
                executionId = executionId
              ))
          }
        })
    result
  }

  /**
   * Execute the mapping on an JSON converted input
   *
   * @param jo                 Input object
   * @param fhirMappingService Mapping service
   * @param errorHandlingType  Error handling type
   * @param executionId        Id of FhirMappingJobExecution object
   * @return
   */
  private def executeMappingOnInput(jo: JObject,
                                    otherInputs: Map[String, JValue],
                                    fhirMappingService: FhirMappingService,
                                    errorHandlingType: ErrorHandlingType,
                                    executionId: Option[String] = None): Seq[FhirMappingResult] = {

    val results =
      try {
        val mappedResources = Await.result(fhirMappingService.mapToFhir(jo, otherInputs), ToFhirConfig.engineConfig.mappingTimeout)
        mappedResources.flatMap {
          //If this is a JSON Patch, the resources are patches so return it as single result
          case (mappingExpr, resources, fhirInteraction) if fhirInteraction.exists(_.`type` == "patch") && resources.length > 1 =>
            Seq(FhirMappingResult(
              jobId = fhirMappingService.jobId,
              mappingUrl = fhirMappingService.mappingUrl,
              mappingExpr = Some(mappingExpr),
              timestamp = Timestamp.from(Instant.now()),
              source = Some(jo.toJson),
              mappedResource = Some(JArray(resources.toList).toJson),
              fhirInteraction = fhirInteraction,
              executionId = executionId
            ))
          //Otherwise return each resource as a separate mapping result
          case (mappingExpr, resources, fhirInteraction) =>
            resources.map(r =>
              FhirMappingResult(
                jobId = fhirMappingService.jobId,
                mappingUrl = fhirMappingService.mappingUrl,
                mappingExpr = Some(mappingExpr),
                timestamp = Timestamp.from(Instant.now()),
                source = Some(jo.toJson),
                mappedResource = Some(r.toJson),
                fhirInteraction = fhirInteraction,
                executionId = executionId
              )
            )
        }
      } catch {
        // Exception in expression evaluation
        case FhirMappingException(mappingExpr, t: FhirExpressionException) =>
          val fmr = FhirMappingResult(
            jobId = fhirMappingService.jobId,
            mappingUrl = fhirMappingService.mappingUrl,
            mappingExpr = Some(mappingExpr),
            timestamp = Timestamp.from(Instant.now()),
            source = Some(jo.toJson),
            error = Some(FhirMappingError(
              code = FhirMappingErrorCodes.MAPPING_ERROR,
              description = t.msg + t.t.map(_.getMessage).map(" " + _).getOrElse(""),
              expression = t.expression
            )),
            executionId = executionId)
          if (errorHandlingType == ErrorHandlingType.CONTINUE) {
            Seq(fmr)
          } else {
            throw FhirMappingException(fmr.toString, t)
          }
        //Other general exceptions
        case e: FhirMappingException =>
          val fmr = FhirMappingResult(
            jobId = fhirMappingService.jobId,
            mappingUrl = fhirMappingService.mappingUrl,
            mappingExpr = None,
            timestamp = Timestamp.from(Instant.now()),
            source = Some(jo.toJson),
            error = Some(FhirMappingError(
              code = FhirMappingErrorCodes.MAPPING_ERROR,
              description = ExceptionUtil.extractExceptionMessages(e)
            )),
            executionId = executionId)
          if (errorHandlingType == ErrorHandlingType.CONTINUE) {
            Seq(fmr)
          } else {
            throw FhirMappingException(fmr.toString, e)
          }
        case e: TimeoutException =>
          val fmr = FhirMappingResult(
            jobId = fhirMappingService.jobId,
            mappingUrl = fhirMappingService.mappingUrl,
            mappingExpr = None,
            timestamp = Timestamp.from(Instant.now()),
            source = Some(jo.toJson),
            error = Some(FhirMappingError(
              code = FhirMappingErrorCodes.MAPPING_TIMEOUT,
              description = s"A single row could not be mapped to FHIR in ${ToFhirConfig.engineConfig.mappingTimeout.toString}!"
            )),
            executionId = executionId)
          if (errorHandlingType == ErrorHandlingType.CONTINUE) {
            logger.debug("Mapping timeout, continuing the processing of mappings...")
            Seq(fmr)
          } else {
            logger.debug("Mapping timeout, halting the mapping execution!")
            throw FhirMappingException(fmr.toString, e)
          }

        case oth: Exception =>
          val fmr = FhirMappingResult(
            jobId = fhirMappingService.jobId,
            mappingUrl = fhirMappingService.mappingUrl,
            mappingExpr = None,
            timestamp = Timestamp.from(Instant.now()),
            source = Some(jo.toJson),
            error = Some(FhirMappingError(
              code = FhirMappingErrorCodes.UNEXPECTED_PROBLEM,
              description = "Exception:" + oth.getMessage
            )),
            executionId = executionId)
          if (errorHandlingType == ErrorHandlingType.CONTINUE) {
            logger.error("Unexpected problem while executing the mappings...", oth)
            Seq(fmr)
          } else {
            logger.error("Unexpected problem while executing the mappings...", oth)
            throw FhirMappingException(fmr.toString, oth)
          }
      }
    results
  }
}
