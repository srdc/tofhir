package io.onfhir.tofhir.engine

import com.typesafe.scalalogging.Logger
import io.onfhir.api.Resource
import io.onfhir.tofhir.config.MappingErrorHandling.MappingErrorHandling
import io.onfhir.tofhir.config.{MappingErrorHandling, ToFhirConfig}
import io.onfhir.tofhir.model.FhirMappingException
import io.onfhir.util.JsonFormatter._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.json4s.JObject
import org.json4s.jackson.Serialization

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
  def executeMapping(spark: SparkSession, df: DataFrame, fhirMappingService: FhirMappingService, errorHandlingType: MappingErrorHandling): Dataset[String] = {
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
                                           errorHandlingType: MappingErrorHandling): Dataset[String] = {
    import spark.implicits._
    val result =
      df
        .flatMap(row => {
          val jo = convertRowToJObject(row)
          val resources = try {
            Await.result(fhirMappingService.mapToFhir(jo), ToFhirConfig.mappingTimeout)
          } catch {
            case e: FhirMappingException =>
              logger.error(e.getMessage, e)
              if (errorHandlingType == MappingErrorHandling.CONTINUE) {
                Seq.empty[Resource]
              } else {
                throw e
              }
            case e: TimeoutException =>
              logger.error(s"TimeoutException. A single row could not be mapped to FHIR in 5 seconds. The row JObject: ${Serialization.write(jo)}")
              if (errorHandlingType == MappingErrorHandling.CONTINUE) {
                logger.debug("Continuing the processing of mappings...")
                Seq.empty[Resource]
              } else {
                logger.debug("Will halt the mapping execution!")
                throw e
              }
          }
          resources.map(_.toJson)
        })
    result
  }
}
