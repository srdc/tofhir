package io.tofhir.server.service

import akka.shapeless.HList.ListCompat.::
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.ToFhirEngine
import io.tofhir.engine.config.{ErrorHandlingType, ToFhirConfig}
import io.tofhir.engine.mapping.{FhirMappingJobManager, MappingContextLoader}
import io.tofhir.engine.model._
import io.tofhir.engine.util.FileUtils
import io.tofhir.engine.util.FileUtils.FileExtensions
import io.tofhir.server.config.SparkConfig
import io.tofhir.server.model.{ResourceNotFound, TestResourceCreationRequest}
import io.tofhir.server.service.job.IJobRepository
import io.tofhir.server.service.mapping.IMappingRepository
import io.tofhir.server.service.schema.ISchemaRepository
import io.tofhir.server.util.DataFrameUtil
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{ArrayType, BinaryType, IntegerType, ObjectType, StringType, StructField, StructType}
import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods

import java.io.File
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * Service to handle all execution related operations
 * E.g. Run a mapping job, run a mapping task, run a test resource creation, get execution logs
 *
 * @param jobRepository
 * @param mappingRepository
 * @param schemaRepository
 */
class ExecutionService(jobRepository: IJobRepository, mappingRepository: IMappingRepository, schemaRepository: ISchemaRepository) extends LazyLogging {

  val toFhirEngine = new ToFhirEngine(Some(mappingRepository), Some(schemaRepository))

  val fhirMappingJobManager =
    new FhirMappingJobManager(
      toFhirEngine.mappingRepo,
      toFhirEngine.contextLoader,
      toFhirEngine.schemaLoader,
      toFhirEngine.functionLibraries,
      toFhirEngine.sparkSession,
      ErrorHandlingType.CONTINUE
    )

  /**
   * Run the job for the given mapping tasks
   *
   * @param projectId   project id the job belongs to
   * @param jobId       job id
   * @param mappingUrls the mapping tasks to be executed
   * @return
   */
  def runJob(projectId: String, jobId: String, mappingUrls: Option[Seq[String]] = None): Future[Unit] = {
    if (!jobRepository.getCachedMappingsJobs.contains(projectId) || !jobRepository.getCachedMappingsJobs(projectId).contains(jobId)) {
      throw ResourceNotFound("Mapping job does not exists.", s"A mapping job with id $jobId does not exists in the mapping job repository")
    }

    val mappingJob: FhirMappingJob = jobRepository.getCachedMappingsJobs(projectId)(jobId)

    // get the list of mapping task to be executed
    val mappingTasks = mappingUrls match {
      case Some(urls) => urls.flatMap(url => mappingJob.mappings.find(p => p.mappingRef.contentEquals(url)))
      case None => mappingJob.mappings
    }
    // create execution
    val mappingJobExecution = FhirMappingJobExecution(jobId = mappingJob.id, projectId = projectId, mappingTasks = mappingTasks)
    if (mappingJob.sourceSettings.exists(_._2.asStream)) {
      Future { // TODO we lose the ability to stop the streaming job
        val streamingQuery =
          fhirMappingJobManager
            .startMappingJobStream(
              mappingJobExecution,
              sourceSettings = mappingJob.sourceSettings,
              sinkSettings = mappingJob.sinkSettings,
              terminologyServiceSettings = mappingJob.terminologyServiceSettings,
              identityServiceSettings = mappingJob.getIdentityServiceSettings()
            )
        streamingQuery.awaitTermination()
      }
    } else {
      fhirMappingJobManager
        .executeMappingJob(
          mappingJobExecution = mappingJobExecution,
          sourceSettings = mappingJob.sourceSettings,
          sinkSettings = mappingJob.sinkSettings,
          terminologyServiceSettings = mappingJob.terminologyServiceSettings,
          identityServiceSettings = mappingJob.getIdentityServiceSettings()
        )
    }
  }

  /**
   * Tests the given mapping task by running it with mapping job configurations (i.e. source data configurations) and
   * returns its results
   *
   * @param projectId                   project id the job belongs to
   * @param jobId                       job id
   * @param testResourceCreationRequest test resource creation request to be executed
   * @return
   */
  def testMappingWithJob(projectId: String, jobId: String, testResourceCreationRequest: TestResourceCreationRequest): Future[Seq[FhirMappingResult]] = {
    if (!jobRepository.getCachedMappingsJobs.contains(projectId) || !jobRepository.getCachedMappingsJobs(projectId).contains(jobId)) {
      throw ResourceNotFound("Mapping job does not exists.", s"A mapping job with id $jobId does not exists in the mapping job repository.")
    }
    val mappingJob: FhirMappingJob = jobRepository.getCachedMappingsJobs(projectId)(jobId)

    // If an unmanaged mapping is provided within the mapping task, normalize the context urls
    val mappingTask: FhirMappingTask =
      testResourceCreationRequest.fhirMappingTask.mapping match {
        case None => testResourceCreationRequest.fhirMappingTask
        case _ =>
          // get the path of mapping file which will be used to normalize mapping context urls
          val pathToMappingFile: File = FileUtils.getPath(ToFhirConfig.engineConfig.mappingRepositoryFolderPath, projectId, s"${testResourceCreationRequest.fhirMappingTask.mapping.get.id}${FileExtensions.JSON}").toFile
          // normalize the mapping context urls
          val mappingWithNormalizedContextUrls: FhirMapping = MappingContextLoader.normalizeContextURLs(Seq((testResourceCreationRequest.fhirMappingTask.mapping.get, pathToMappingFile))).head
          // Copy the mapping with the normalized urls
          testResourceCreationRequest.fhirMappingTask.copy(mapping = Some(mappingWithNormalizedContextUrls))
      }

    val (fhirMapping, dataSourceSettings, dataFrame) = fhirMappingJobManager.readJoinSourceData(mappingTask, mappingJob.sourceSettings)
    val selected = DataFrameUtil.applyResourceFilter(dataFrame, testResourceCreationRequest.resourceFilter)
    fhirMappingJobManager.executeTask(mappingJob.id, fhirMapping, selected, dataSourceSettings, mappingJob.terminologyServiceSettings, mappingJob.getIdentityServiceSettings())
      .map { dataFrame =>
        dataFrame
          .collect() // Collect into an Array[String]
          .toSeq // Convert to Seq[Resource]
      }
  }

  /**
   * Returns the logs of mapping tasks ran in the given execution.
   *
   * @param executionId the identifier of mapping job execution.
   * @return the logs of mapping tasks
   * */
  def getExecutionLogs(executionId: String): Future[Seq[JValue]] = {
    Future {
      // read logs/tofhir-mappings.log file
      val dataFrame = SparkConfig.sparkSession.read.json("logs/tofhir-mappings.log")
      // handle the case where no job has been run yet which makes the data frame empty
      if (dataFrame.isEmpty) {
        Seq.empty
      }
      else {
        // Get job run logs for the given execution. ProjectId field is not null for selecting jobRunsLogs, filter out job error logs.
        val jobRunLogs = dataFrame.filter(s"executionId = '$executionId' and projectId is not null")

        // Handle the case where the job has not been run yet, which makes the data frame empty
        if (jobRunLogs.isEmpty) {
          Seq.empty
        } else {
          // Get error logs for the given execution and select needed columns. ProjectId field is null for selecting job error logs, filter out jobRunsLogs.
          val jobErrorLogs = dataFrame.filter(s"executionId = '$executionId' and projectId is null").select(List("errorCode", "errorDesc", "message", "mappingUrl").map(col):_*)

          // Group job error logs by mapping url
          val jobErrorLogsGroupedByMappingUrl = jobErrorLogs.groupByKey(row => row.get(row.fieldIndex("mappingUrl")).toString)(Encoders.STRING)

          // Collect job run logs for matching with mappingUrl field of job error logs
          var jobRunLogsData = jobRunLogs.collect()

          // Add error details to job run logs if any error occurred while executing the job.
          val jobRunLogsWithErrorDetails = jobErrorLogsGroupedByMappingUrl.mapGroups((key, values) => {
            // Find the related job run log to given mapping url
            val jobRunLog = jobRunLogsData.filter(row => row.getAs[String]("mappingUrl") == key)
            // Append error logs to the related job run log
            Row.fromSeq(Row.unapplySeq(jobRunLog.head).get :+ values.toSeq)
          })(
              // Define a new schema for the resulting rows and create an encoder for it. We will add a "error_logs" column to job run logs that contains related error logs.
              RowEncoder(jobRunLogs.schema.add("error_logs", ArrayType(
                  new StructType()
                    .add("errorCode", StringType)
                    .add("errorDesc", StringType)
                    .add("message", StringType)
                    .add("mappingUrl", StringType)
                ))
              )
          )

          // Build a map for updated job run logs (mappingUrl -> jobRunLogsWithErrorDetails)
          val updatedJobRunLogsMap = jobRunLogsWithErrorDetails.collect().map(updatedJobRunLog =>
            updatedJobRunLog.getAs[String]("mappingUrl") -> updatedJobRunLog).toMap

          // Replace job run logs if it is in the map
          jobRunLogsData = jobRunLogsData.map(jobRunLog =>
            updatedJobRunLogsMap.getOrElse(jobRunLog.getAs[String]("mappingUrl"), jobRunLog))

          // return json objects for job run logs
          jobRunLogsData.map(row => {
            JsonMethods.parse(row.json)
          })
        }
      }
    }
  }

  /**
   * Returns the list of mapping job executions. It extracts the logs from {@link logs/ tofhir - mappings.log} file for
   * the given mapping job and groups them by their execution id and returns a single log for each execution. Further,
   * it applies the pagination to the resulting execution logs.
   *
   * @param projectId   project id the job belongs to
   * @param jobId       job id
   * @param queryParams parameters to filter results such as paging
   * @return a tuple as follows
   *         first element is the execution logs of mapping job as a JSON array. It returns an empty array if the job has not been run before.
   *         second element is the total number of executions without applying any filters i.e. query params
   * @throws ResourceNotFound when mapping job does not exist
   */
  def getExecutions(projectId: String, jobId: String, queryParams: Map[String, String]): Future[(Seq[JValue], Long)] = {
    // retrieve the job to validate its existence
    jobRepository.getJob(projectId, jobId).map {
      case Some(_) =>
        // read logs/tofhir-mappings.log file
        val dataFrame = SparkConfig.sparkSession.read.json("logs/tofhir-mappings.log")
        // handle the case where no job has been run yet which makes the data frame empty
        if (dataFrame.isEmpty) {
          (Seq.empty, 0)
        }
        else {
          val jobRuns = dataFrame.filter(s"jobId = '$jobId' and projectId = '$projectId'")
          // handle the case where the job has not been run yet which makes the data frame empty
          if (jobRuns.isEmpty) {
            (Seq.empty, 0)
          } else {
            // group logs by execution id
            val jobRunsGroupedByExecutionId = jobRuns.groupByKey(row => row.get(row.fieldIndex("executionId")).toString)(Encoders.STRING)
            // get execution logs
            val executionLogs = jobRunsGroupedByExecutionId.mapGroups((key, values) => {
              // keeps the rows belonging to this execution
              val rows = values.toList
              val count = rows.length
              val successCount = rows.count(r => r.get(r.fieldIndex("result")).toString.contentEquals("SUCCESS"))
              // use the timestamp of first one, which is ran first, as timestamp of execution
              val timestamp = rows.head.get(rows.head.fieldIndex("@timestamp")).toString
              // set the status of execution
              var status = "SUCCESS"
              if (successCount == 0) {
                status = "FAILURE"
              } else if (successCount != count) {
                status = "PARTIAL_SUCCESS"
              }
              Row.fromSeq(Seq(key, count, timestamp, status))
            })(RowEncoder(StructType(
              StructField("id", StringType) ::
                StructField("mappingTaskCount", IntegerType) ::
                StructField("timestamp", StringType) ::
                StructField("status", StringType) :: Nil
            )))
            // page size is 10, handle pagination
            val total = executionLogs.count()
            val numOfPages = Math.ceil(total.toDouble / 10).toInt
            val page = queryParams.getOrElse("page", "1").toInt
            // handle the case where requested page does not exist
            if (page > numOfPages) {
              (Seq.empty, 0)
            } else {
              val start = (page - 1) * 10
              val end = Math.min(start + 10, total.toInt)
              // sort the executions by latest to oldest
              val paginatedLogs = executionLogs.sort(executionLogs.col("timestamp").desc).collect().slice(start, end)
              (paginatedLogs.map(row => {
                JsonMethods.parse(row.json)
              }), total)
            }
          }
        }
      case None => throw ResourceNotFound("Mapping job does not exists.", s"A mapping job with id $jobId does not exists")
    }
  }
}
