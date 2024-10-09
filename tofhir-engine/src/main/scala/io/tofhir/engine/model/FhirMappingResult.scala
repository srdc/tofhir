package io.tofhir.engine.model

import ch.qos.logback.more.appenders.marker.MapMarker

import java.sql.Timestamp


/**
 * Mapping process result
 *
 * @param jobId               Identifier of the job that this mapping is performed within
 * @param mappingTaskName     Name of the mappingTask that is executed
 * @param mappingExpr         Name of the mapping expression (FhirMapping.mapping.expression.name) that this mapping is based on
 * @param timestamp           Timestamp of the result
 * @param mappedResource      If successful, JSON serialization of the FHIR resource generated via the mapping
 * @param source              If there is a problem in the process, the JSON serialization of the source data
 * @param error               If there is a problem in the process, description of the problem
 * @param fhirInteraction     FHIR interaction details to persist the mapped result
 * @param executionId         Id of FhirMappingJobExecution object
 * @param projectId           Project identifier associated with the mapping job
 * @param resourceType        The type of FHIR resource generated by the mapping. This field is optional, as the
 *                            FHIR mapping result may represent a flat structure without a specific FHIR resource type.
 * @param mappedFhirResources List of mapped FHIR resources. This is used to group mapped resources by input row for testing purposes.
 */
case class FhirMappingResult(
                              jobId:String,
                              mappingTaskName:String,
                              mappingExpr:Option[String] = None,
                              timestamp:Timestamp,
                              mappedResource:Option[String] = None,
                              source:String,
                              error:Option[FhirMappingError] = None,
                              fhirInteraction:Option[FhirInteraction] = None,
                              executionId: Option[String] = None,
                              projectId: Option[String] = None,
                              resourceType: Option[String] = None,
                              mappedFhirResources: Seq[MappedFhirResource] = Seq.empty,
                            ) {
  final val eventId:String = "MAPPING_RESULT"
  override def toString: String = {
    s"Mapping failure (${error.get.code}) for job '$jobId' and mappingTask '$mappingTaskName'${mappingExpr.map(e => s" within expression '$e'").getOrElse("")} execution '${executionId.getOrElse("")}'!\n"+
    s"\tSource: ${source}\n"+
    s"\tError: ${error.get.description}" +
      error.get.expression.map(e =>  s"\n\tExpression: $e").getOrElse("")
  }

  /**
   * Converts the FhirMappingResult to a MapMarker.
   *
   * @return The MapMarker object representing the FhirMappingResult.
   */
  def toMapMarker: MapMarker = {
    // create a new HashMap to store the marker attributes
    val markerMap: java.util.Map[String, Any] = new java.util.HashMap[String, Any]()
    // add attributes to the marker map
    markerMap.put("jobId", jobId)
    markerMap.put("projectId", projectId.getOrElse(""))
    markerMap.put("executionId", executionId.getOrElse(""))
    markerMap.put("mappingTaskName", mappingTaskName)
    markerMap.put("mappingExpr", mappingExpr.orElse(null))
    markerMap.put("source", source)
    markerMap.put("errorCode", error.get.code)
    markerMap.put("errorDesc", error.get.description)
    markerMap.put("errorExpr", error.get.expression.getOrElse(""))
    markerMap.put("eventId", eventId)
    // The current timestamp is automatically added to the log entry when it is sent to Elasticsearch or written to a file.
    // As a result, there is no need to manually add a "@timestamp" field.
    // However, during the process of writing the log to Elasticsearch, the timestamp is rounded, resulting in a loss of precision.
    // For example, "2024-08-28_13:54:44.740" may be rounded to "2024-08-28_13:54:44.000" in Elasticsearch.
    // This rounding leads to the loss of crucial millisecond information, which is important for accurately sorting logs.
    markerMap.put("@timestamp", System.currentTimeMillis.toString)
    // create a new MapMarker using the marker map
    val marker: MapMarker = new MapMarker("marker", markerMap)
    // add mappedResource to the marker map if error code is INVALID_RESOURCE
    if (mappedResource.isDefined && error.get.code == FhirMappingErrorCodes.INVALID_RESOURCE)
      markerMap.put("mappedResource", mappedResource.get)
    marker
  }

}

/**
 * Description of the error occurred during the mapping process
 * @param code          Code for the error (category)
 * @param description   Description of the problem
 * @param expression    Mapping expression that problem is occurred (e.g. FHIR Path expression)
 */
case class FhirMappingError(code:String, description:String, expression:Option[String] = None)

/**
 * List of error codes in the mapping process
 */
object FhirMappingErrorCodes {
  //If the input data is invalid (not conforming to given schema)
  final val INVALID_INPUT = "invalid_input"
  // If the mapping process is timed out
  final val MAPPING_TIMEOUT = "mapping_timeout"
  // If there is any problem during execution of mapping expressions
  final val MAPPING_ERROR = "mapping_error"
  // FHIR server rejects the resource creation as it is invalid
  final val INVALID_RESOURCE = "invalid_resource"
  // Timeout in FHIR interaction to create the resource
  final val FHIR_API_TIMEOUT = "fhir_api_timeout"
  // Problem in Terminology or Identity service interactions
  final val SERVICE_PROBLEM = "service_error"

  final val UNEXPECTED_PROBLEM = "unexpected_problem"
}

/**
 * Mapped FHIR resources model after the mapping process, only used for mapping testing feature
 *
 * @param mappingExpr     Mapping expression name that this mapping is based on
 * @param mappedResource  JSON serialization of the FHIR resource generated via the mapping
 * @param fhirInteraction FHIR interaction details
 */
case class MappedFhirResource(mappingExpr:Option[String] = None,
                              mappedResource: Option[String] = None,
                              fhirInteraction: Option[FhirInteraction])