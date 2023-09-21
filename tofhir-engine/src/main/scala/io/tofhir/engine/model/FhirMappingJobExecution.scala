package io.tofhir.engine.model

import io.tofhir.engine.config.{ErrorHandlingType, ToFhirConfig}
import io.tofhir.engine.config.ErrorHandlingType.ErrorHandlingType

import java.util.UUID
import java.util.regex.Pattern

/**
 * Represents the execution of mapping tasks included in a mapping job.
 *
 * @param id                   Unique identifier for the execution
 * @param projectId            Unique identifier of project to which mapping job belongs
 * @param job                  Fhir mapping job
 * @param mappingTasks         List of mapping tasks to be executed
 * @param mappingErrorHandling Error handling type for execution process
 */
case class FhirMappingJobExecution(id: String = UUID.randomUUID().toString,
                                   projectId: String = "",
                                   job: FhirMappingJob,
                                   mappingTasks: Seq[FhirMappingTask] = Seq.empty,
                                   mappingErrorHandling: ErrorHandlingType = ErrorHandlingType.CONTINUE
                                  ) {
  /**
   * Creates a checkpoint directory for a mapping included in a job
   *
   * @param mappingUrl Url of the mapping
   * @return Directory path in which the checkpoints will be managed
   */
  def getCheckpointDirectory(mappingUrl: String): String =
    s"./checkpoint/${job.id}/${mappingUrl.hashCode}"

  /**
   * Creates a error output directory for a mapping execution included in a job and an execution
   * error-folder-path\<error-type>\job-<jobId>\execution-<executionId>\<hashedMappingUrl>\<random-generated-name-by-spark>.csv
   * e.g. error-folder-path\invalid_input\job-d13b5044-f05c-4698-86c2-d83b3c5083f8\execution-59733de5-1c92-4741-b032-6e9e13ee4550\-521848504\part-00000-1d7d9467-0195-4d28-964d-89171727fa41-c000.csv
   *
   * @param mappingUrl
   * @param errorType
   * @return
   */
  def getErrorOutputDirectory(mappingUrl: String, errorType: String): String =
    s"${ToFhirConfig.engineConfig.erroneousRecordsFolder}/${errorType}/job-${job.id}/execution-${id}/${this.convertUrlToAlphaNumeric(mappingUrl)}"


  /**
   * Convert mapping url to alphanumeric string to be used as folder name
   * e.g. https://aiccelerate.eu/fhir/mappings/streaming-test/patient-mapping -> https_aiccelerate_eu_fhir_mappings_streaming_test_patient_mapping
   * @param url
   * @return
   */
  private def convertUrlToAlphaNumeric(url: String): String = {
    // Define a regular expression pattern to match alphanumeric parts
    val pattern = Pattern.compile("[a-zA-Z0-9]+")
    val matcher = pattern.matcher(url)
    // Extract meaningful words and transform them
    val extractedWords = scala.collection.mutable.ListBuffer[String]()
    while (matcher.find()) {
      val word = matcher.group()
      // You can further transform the word if needed (e.g., removing spaces or special characters)
      // For simplicity, we are not doing any additional transformation here.
      extractedWords += word
    }
    // Combine the transformed words to create a folder name
    extractedWords.mkString("-")
  }

}
