package io.tofhir.server.model

/**
 * Log information of the result of a mapping job
 *
 * @param jobId              Job identifier
 * @param timestamp          The time when job was run
 * @param message            Log message
 * @param result             Result of the job
 * @param mappingUrl         Url of the mapping
 * @param numOfInvalids      Total number of invalid input rows
 * @param numOfNotMapped     Total number of mappings per row that is not successful
 * @param numOfFhirResources Total number of FHIR resources created as a result mapping(s)
 * @param numOfFailedWrites  Total number of FHIR resources that cannot be written to the configured sink (e.g. FHIR repository)
 */
case class FhirMappingJobLog(jobId: String,
                             timestamp: String,
                             message: String,
                             result: String,
                             mappingUrl: Option[String],
                             numOfInvalids: Long = -1,
                             numOfNotMapped: Long = -1,
                             numOfFhirResources: Long = -1,
                             numOfFailedWrites: Long = -1
                            )