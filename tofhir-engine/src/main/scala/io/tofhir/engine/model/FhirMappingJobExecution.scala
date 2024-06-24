package io.tofhir.engine.model

import io.tofhir.engine.config.ToFhirConfig
import io.tofhir.engine.util.SparkUtil
import org.apache.spark.sql.streaming.StreamingQuery

import java.nio.file.Paths
import java.util.UUID
import java.util.regex.Pattern

/**
 * Represents the execution of mapping tasks included in a mapping job.
 *
 * @param id                         Unique identifier for the execution
 * @param projectId                  Unique identifier of project to which mapping job belongs
 * @param job                        Fhir mapping job
 * @param mappingTasks               List of mapping tasks to be executed (as a subset of the mapping tasks defined in the job)
 * @param jobGroupIdOrStreamingQuery Keeps Spark job group id for batch jobs and StreamingQuery for streaming jobs
 */
// TODO: The FhirMappingJobExecution model currently includes the entire FhirMappingJob ('job' field), which is unnecessary.
//  We should remove the 'job' field from the model. Instead, add only the necessary fields to the model.
case class FhirMappingJobExecution(id: String = UUID.randomUUID().toString,
                                   projectId: String = "",
                                   job: FhirMappingJob,
                                   mappingTasks: Seq[FhirMappingTask] = Seq.empty,
                                   jobGroupIdOrStreamingQuery: Option[Either[String, collection.mutable.Map[String, StreamingQuery]]] = None
                                  ) {

  /**
   * Returns whether the execution is streaming or not
   * @return
   */
  def isStreaming(): Boolean = {
    job.sourceSettings.exists(source => source._2.asStream)
  }

  /**
   * Returns the map of streaming queries i.e. map of (mapping url -> streaming query)
   * @return
   */
  def getStreamingQueryMap(): collection.mutable.Map[String, StreamingQuery] = {
    jobGroupIdOrStreamingQuery match {
      case Some(value) => value match {
        case Right(queryMap) => queryMap
        case Left(_) => throw new IllegalStateException("Trying to access StreamingQuery map, but job group id exists instead")
      }
      case None => throw new IllegalStateException("Trying to access StreamingQuery map, but it does not exist")
    }
  }

  /**
   * Returns the [[StreamingQuery]] from this execution, if any. It throws a [[IllegalStateException]], if the query is not available.
   * @return
   */
  def getStreamingQuery(mappingUrl: String): StreamingQuery = {
    getStreamingQueryMap().get(mappingUrl) match {
      case Some(query) => query
      case None => throw new IllegalStateException(s"Trying to access StreamingQuery, but none exists for the given mapping url: $mappingUrl")
    }
  }

  /**
   * Returns the Spark job group if from this execution, if any. It throws a [[IllegalStateException]], if the job id is not available.
   * @return
   */
  def getJobGroupId(): String = {
    jobGroupIdOrStreamingQuery match {
      case Some(value) => value match {
        case Right(_) => throw new IllegalStateException("Trying to access StreamingQuery, but streaming query exists instead")
        case Left(jobGroupId) => jobGroupId
      }
      case None => throw new IllegalStateException("Trying to access job group id, but it does not exist")
    }
  }

  /**
   * Creates a checkpoint directory for a mapping included in a job
   *
   * @param mappingUrl Url of the mapping
   * @return Directory path in which the checkpoints will be managed
   */
  def getCheckpointDirectory(mappingUrl: String): String =
    Paths.get(ToFhirConfig.sparkCheckpointDirectory, job.id, mappingUrl.hashCode.toString).toString

  /**
   * Creates a commit directory for a mapping included in a job
   *
   * @param mappingUrl Url of the mapping
   * @return Directory path in which the commits will be managed
   */
  def getCommitDirectory(mappingUrl: String): String = {
    SparkUtil.getCommitDirectoryPath(Paths.get(getCheckpointDirectory(mappingUrl)))
  }

  /**
   * Creates a source directory for a mapping included in a job
   *
   * @param mappingUrl Url of the mapping
   * @return Directory path in which the sources will be managed
   */
  def getSourceDirectory(mappingUrl: String): String = {
    SparkUtil.getSourceDirectoryPath(Paths.get(getCheckpointDirectory(mappingUrl)))
  }

  /**
   * Creates a error output directory for a mapping execution included in a job and an execution
   * error-folder-path\<error-type>\job-<jobId>\execution-<executionId>\<mappingUrl>\<random-generated-name-by-spark>.csv
   * e.g. error-folder-path\invalid_input\job-d13b5044-f05c-4698-86c2-d83b3c5083f8\execution-59733de5-1c92-4741-b032-6e9e13ee4550\-521848504\part-00000-1d7d9467-0195-4d28-964d-89171727fa41-c000.csv
   *
   * @param mappingUrl
   * @param errorType
   * @return
   */
  def getErrorOutputDirectory(mappingUrl: String, errorType: String): String =
    Paths.get(ToFhirConfig.engineConfig.erroneousRecordsFolder, errorType, s"job-${job.id}", s"execution-${id}",
      this.convertUrlToAlphaNumeric(mappingUrl)).toString


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
