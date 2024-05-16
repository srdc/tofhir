package io.tofhir.engine.mapping

import com.typesafe.scalalogging.Logger
import io.onfhir.api.client.FhirClientException
import io.onfhir.expression.FhirExpressionException
import io.onfhir.path.FhirPathException
import org.json4s.jackson.{JsonMethods, Serialization}
import io.tofhir.common.model.Json4sSupport.formats
import io.tofhir.common.util.ExceptionUtil
import io.tofhir.engine.config.ToFhirConfig
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
   * Converts the input row to a JObject for mapping.
   *
   * If columnToConvert is None, the entire row is converted to JObject using Spark's internal method.
   * If columnToConvert is specified, only the value of the specified column is converted to JObject.
   *
   * @param row             The Row to be converted to JObject.
   * @param columnToConvert An optional column name to convert to JObject. If None, the entire row is converted.
   * @return The converted JObject representing the row or the specified column.
   */
  private def convertRowToJObject(row: Row, columnToConvert: Option[String]): JObject = {
    if(columnToConvert.isEmpty){
      // Row class of Spark has a private method (jsonValue) which transforms a Row into a json4s JObject.
      // We hacked the class to make it accessible and used it to covert a Row into JObject.
      val method = row.getClass.getSuperclass.getInterfaces.apply(0).getMethod("jsonValue")
      method.setAccessible(true)
      method.invoke(row).asInstanceOf[JObject]
    } else {
      // Get the index of the specified column
      val resourceIndex = row.fieldIndex(columnToConvert.get)
      // Access the value of the specified column
      val resourceValue = row.getString(resourceIndex)
      // Parse the JSON string into a JObject
      JsonMethods.parse(resourceValue).asInstanceOf[JObject]
    }
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
  def executeMapping(spark: SparkSession, df: DataFrame, fhirMappingService: FhirMappingService, executionId: Option[String] = None): Dataset[FhirMappingResult] = {
    fhirMappingService.sources match {
      case Seq(_) => executeMappingOnSingleSource(spark, df, fhirMappingService, executionId)
      //Executing on multiple sources
      case oth => executeMappingOnMultipleSources(spark, df, fhirMappingService, oth, executionId)
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
                                           executionId: Option[String] = None): Dataset[FhirMappingResult] = {
    import spark.implicits._
    val result =
      df
        .flatMap(row => {
          val jo =
            convertRowToJObject(row, fhirMappingService.columnToConvert) //convert the row to JSON object
              .removeField(_._1 == SourceHandler.INPUT_VALIDITY_ERROR) //Remove the extra field appended during validation
              .asInstanceOf[JObject]

          Option(row.getAs[String](SourceHandler.INPUT_VALIDITY_ERROR)) match {
            //If input is valid
            case None => executeMappingOnInput(jo, Map.empty[String, JValue], fhirMappingService, executionId)
            //If the input is not valid, return the error
            case Some(validationError) =>
              Seq(FhirMappingResult(
                jobId = fhirMappingService.jobId,
                mappingUrl = fhirMappingService.mappingUrl,
                mappingExpr = None,
                timestamp = Timestamp.from(Instant.now()),
                source = Some(Serialization.write(jo)),
                error = Some(FhirMappingError(
                  code = FhirMappingErrorCodes.INVALID_INPUT,
                  description = validationError
                )),
                executionId = executionId,
                projectId = fhirMappingService.projectId
              ))
          }
        })
    result
  }

  private def executeMappingOnMultipleSources(spark: SparkSession,
                                              df: DataFrame,
                                              fhirMappingService: FhirMappingService,
                                              sources: Seq[String],
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
            convertRowToJObject(mainSource, fhirMappingService.columnToConvert) //convert the row to JSON object
              .removeField(f => f._1.startsWith("__")) //Remove the extra field appended during validation and other source objects
              .asInstanceOf[JObject]
          //Parse data coming from other sources as context parameters
          val otherObjectMap: Map[String, JValue] =
            otherSourceRows
              .flatMap {
                case (alias, rows) =>
                  rows
                    .map(r => convertRowToJObject(r, fhirMappingService.columnToConvert).removeField(f => f._1.startsWith("__"))) match {
                    case Nil => None
                    case Seq(o) => Some(alias -> o)
                    case oth => Some(alias -> JArray(oth))
                  }
              }
              .toMap

          validationErrors match {
            //If input is valid
            case Nil => executeMappingOnInput(jo, otherObjectMap, fhirMappingService, executionId)
            //If the input is not valid, return the error
            case _ =>
              Seq(FhirMappingResult(
                jobId = fhirMappingService.jobId,
                mappingUrl = fhirMappingService.mappingUrl,
                mappingExpr = None,
                timestamp = Timestamp.from(Instant.now()),
                source = Some(Serialization.write(jo)),
                error = Some(FhirMappingError(
                  code = FhirMappingErrorCodes.INVALID_INPUT,
                  description = validationErrors.mkString("\n")
                )),
                executionId = executionId,
                projectId = fhirMappingService.projectId
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
   * @param executionId        Id of FhirMappingJobExecution object
   * @return
   */
  private def executeMappingOnInput(jo: JObject,
                                    otherInputs: Map[String, JValue],
                                    fhirMappingService: FhirMappingService,
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
              source = Some(Serialization.write(jo)),
              mappedResource = Some(Serialization.write(JArray(resources.toList))),
              fhirInteraction = fhirInteraction,
              executionId = executionId,
              projectId = fhirMappingService.projectId
            ))
          //Otherwise return each resource as a separate mapping result
          case (mappingExpr, resources, fhirInteraction) =>
            resources.map(r =>
              FhirMappingResult(
                jobId = fhirMappingService.jobId,
                mappingUrl = fhirMappingService.mappingUrl,
                mappingExpr = Some(mappingExpr),
                timestamp = Timestamp.from(Instant.now()),
                source = Some(Serialization.write(jo)),
                mappedResource = Some(Serialization.write(r)),
                fhirInteraction = fhirInteraction,
                executionId = executionId,
                projectId = fhirMappingService.projectId
              )
            )
        }
      } catch {
        // Exception in expression evaluation
        case FhirMappingException(mappingExpr, t: FhirExpressionException) =>
          // if we make use of Identity Service functions such as idxs:resolveIdentifier in the mapping expression,
          // we get a FhirClientException when it cannot connect to Identity Service. In this case, we need to include
          // response body and status in the error description
          var errorDescription = t.msg + t.t.map(_.getMessage).map(" " + _).getOrElse("")
          if(t.t.nonEmpty && t.t.get.isInstanceOf[FhirPathException] && t.t.get.asInstanceOf[FhirPathException].getCause.isInstanceOf[FhirClientException]){
            val innerException = t.t.get.asInstanceOf[FhirPathException].getCause.asInstanceOf[FhirClientException]
            if(innerException.serverResponse.nonEmpty){
              val serverResponse = innerException.serverResponse.get
              errorDescription = errorDescription + s" Status Code: ${serverResponse.httpStatus}${serverResponse.responseBody.map(s" Response Body: " + _).getOrElse("")}"
              // add outcome issues to the error description
              if(serverResponse.outcomeIssues.nonEmpty) {
                errorDescription = errorDescription + s" Outcome Issues: ${serverResponse.outcomeIssues}"
              }
            }
          }
          Seq(FhirMappingResult(
            jobId = fhirMappingService.jobId,
            mappingUrl = fhirMappingService.mappingUrl,
            mappingExpr = Some(mappingExpr),
            timestamp = Timestamp.from(Instant.now()),
            source = Some(Serialization.write(jo)),
            error = Some(FhirMappingError(
              code = FhirMappingErrorCodes.MAPPING_ERROR,
              description = errorDescription,
              expression = t.expression
            )),
            executionId = executionId,
            projectId = fhirMappingService.projectId))
        //Other general exceptions
        case e: FhirMappingException =>
          Seq(FhirMappingResult(
            jobId = fhirMappingService.jobId,
            mappingUrl = fhirMappingService.mappingUrl,
            mappingExpr = None,
            timestamp = Timestamp.from(Instant.now()),
            source = Some(Serialization.write(jo)),
            error = Some(FhirMappingError(
              code = FhirMappingErrorCodes.MAPPING_ERROR,
              description = ExceptionUtil.extractExceptionMessages(e)
            )),
            executionId = executionId,
            projectId = fhirMappingService.projectId))
        case e: TimeoutException =>
          logger.debug("Mapping timeout, continuing the processing of mappings...")
          Seq(FhirMappingResult(
            jobId = fhirMappingService.jobId,
            mappingUrl = fhirMappingService.mappingUrl,
            mappingExpr = None,
            timestamp = Timestamp.from(Instant.now()),
            source = Some(Serialization.write(jo)),
            error = Some(FhirMappingError(
              code = FhirMappingErrorCodes.MAPPING_TIMEOUT,
              description = s"A single row could not be mapped to FHIR in ${ToFhirConfig.engineConfig.mappingTimeout.toString}!"
            )),
            executionId = executionId,
            projectId = fhirMappingService.projectId))
        case oth: Exception =>
          logger.error("Unexpected problem while executing the mappings...", oth)
          Seq(FhirMappingResult(
            jobId = fhirMappingService.jobId,
            mappingUrl = fhirMappingService.mappingUrl,
            mappingExpr = None,
            timestamp = Timestamp.from(Instant.now()),
            source = Some(Serialization.write(jo)),
            error = Some(FhirMappingError(
              code = FhirMappingErrorCodes.UNEXPECTED_PROBLEM,
              description = "Exception:" + oth.getMessage
            )),
            executionId = executionId,
            projectId = fhirMappingService.projectId))
      }
    results
  }
}
