package io.tofhir.engine.model

import io.tofhir.engine.config.ToFhirConfig
import io.tofhir.engine.model.ArchiveModes.ArchiveModes
import io.tofhir.engine.util.SparkUtil
import org.apache.spark.sql.streaming.StreamingQuery

import java.nio.file.Paths
import java.util.UUID
import java.util.regex.Pattern

/**
 * Represents the execution of mapping tasks included in a mapping job.
 *
 * @param id                             Unique identifier for the execution
 * @param projectId                      Unique identifier of project to which mapping job belongs
 * @param mappingTasks                   List of mapping tasks to be executed (as a subset of the mapping tasks defined in the job)
 * @param jobGroupIdOrStreamingQuery     Keeps Spark job group id for batch jobs and StreamingQuery for streaming jobs
 * @param isStreamingJob                 Whether the execution is streaming or not
 * @param fileSystemSourceDataFolderPath If execution has a file system source, this is data folder path of it
 * @param archiveMode                    Archive mode of execution
 * @param saveErroneousRecords           Whether to save erroneous records or not
 * @param isScheduledJob                 Whether the execution is scheduled or not
 */
case class FhirMappingJobExecution(id: String,
                                   projectId: String,
                                   jobId: String,
                                   mappingTasks: Seq[FhirMappingTask],
                                   jobGroupIdOrStreamingQuery: Option[Either[String, collection.mutable.Map[String, StreamingQuery]]],
                                   isStreamingJob: Boolean,
                                   fileSystemSourceDataFolderPath: Option[String],
                                   archiveMode: ArchiveModes,
                                   saveErroneousRecords: Boolean,
                                   isScheduledJob: Boolean
                                  ) {
  /**
   * Returns the map of streaming queries i.e. map of (mappingTask name -> streaming query)
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
  def getStreamingQuery(mappingTaskName: String): StreamingQuery = {
    getStreamingQueryMap().get(mappingTaskName) match {
      case Some(query) => query
      case None => throw new IllegalStateException(s"Trying to access StreamingQuery, but none exists for the given mappingTask name: $mappingTaskName")
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
   * @param mappingTaskName Name of the mappingTask
   * @return Directory path in which the checkpoints will be managed
   */
  def getCheckpointDirectory(mappingTaskName: String): String =
    Paths.get(ToFhirConfig.sparkCheckpointDirectory, jobId, mappingTaskName.hashCode.toString).toString

  /**
   * Creates a commit directory for a mapping included in a job
   *
   * @param mappingTaskName Name of the mappingTask
   * @return Directory path in which the commits will be managed
   */
  def getCommitDirectory(mappingTaskName: String): String = {
    SparkUtil.getCommitDirectoryPath(Paths.get(getCheckpointDirectory(mappingTaskName)))
  }

  /**
   * Creates a source directory for a mapping included in a job
   *
   * @param mappingTaskName Name of the mappingTask
   * @return Directory path in which the sources will be managed
   */
  def getSourceDirectory(mappingTaskName: String): String = {
    SparkUtil.getSourceDirectoryPath(Paths.get(getCheckpointDirectory(mappingTaskName)))
  }

  /**
   * Creates a error output directory for a mapping execution included in a job and an execution
   * error-folder-path\<error-type>\job-<jobId>\execution-<executionId>\<mappingTaskName>\<random-generated-name-by-spark>.csv
   * e.g. error-folder-path\invalid_input\job-d13b5044-f05c-4698-86c2-d83b3c5083f8\execution-59733de5-1c92-4741-b032-6e9e13ee4550\-521848504\part-00000-1d7d9467-0195-4d28-964d-89171727fa41-c000.csv
   *
   * @param mappingTaskName
   * @param errorType
   * @return
   */
  def getErrorOutputDirectory(mappingTaskName: String, errorType: String): String =
    Paths.get(ToFhirConfig.engineConfig.erroneousRecordsFolder, errorType, s"job-${jobId}", s"execution-${id}",
      mappingTaskName).toString

}

/**
 * An object to create FhirMappingJobExecution instances
 */
object FhirMappingJobExecution {

  /**
   *
   * @param id                             Unique identifier for the execution
   * @param projectId                      Unique identifier of project to which mapping job belongs
   * @param job                            FHIR mapping job that includes this execution.
   * @param mappingTasks                   List of mapping tasks to be executed (as a subset of the mapping tasks defined in the job)
   * @param jobGroupIdOrStreamingQuery     Keeps Spark job group id for batch jobs and StreamingQuery for streaming jobs
   * @return
   */
  def apply(id: String = UUID.randomUUID().toString,
            projectId: String = "",
            job: FhirMappingJob,
            mappingTasks: Seq[FhirMappingTask] = Seq.empty,
            jobGroupIdOrStreamingQuery: Option[Either[String, collection.mutable.Map[String, StreamingQuery]]] = None
           ): FhirMappingJobExecution = {

    // Configure properties related to the source settings of the job
    var isStreamingJob = false
    var fileSystemSourceDataFolderPath: Option[String] = None
    if (job.sourceSettings.nonEmpty) {
      isStreamingJob = job.sourceSettings.exists(source => source._2.asStream)
      fileSystemSourceDataFolderPath  = job.sourceSettings.head._2 match {
        case settings: FileSystemSourceSettings => Some(settings.dataFolderPath)
        case _ => None
      }
    }

    // Configure properties related to the data processing settings of the job
    var archiveMode = ArchiveModes.OFF
    var saveErroneousRecords = false
    if(job.dataProcessingSettings != null) {
      archiveMode = job.dataProcessingSettings.archiveMode
      saveErroneousRecords = job.dataProcessingSettings.saveErroneousRecords
    }
    // check whether it is a scheduled job or not
    val isScheduledJob = job.schedulingSettings.nonEmpty

    // Create a FhirMappingJobExecution with only necessary properties
    FhirMappingJobExecution(id, projectId, job.id, mappingTasks, jobGroupIdOrStreamingQuery, isStreamingJob, fileSystemSourceDataFolderPath, archiveMode, saveErroneousRecords, isScheduledJob)
  }
}

