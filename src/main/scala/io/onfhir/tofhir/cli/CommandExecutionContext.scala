package io.onfhir.tofhir.cli

import io.onfhir.tofhir.model.FhirMappingJob
import org.apache.spark.sql.SparkSession

class CommandExecutionContext(val sparkSession: SparkSession,
                              val fhirMappingJob: Option[FhirMappingJob] = None,
                              val mappingNameUrlMap: Map[String, String] = Map.empty) {

}
