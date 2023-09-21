package io.tofhir.server.service

import com.typesafe.scalalogging.LazyLogging
import io.onfhir.path.IFhirPathFunctionLibraryFactory
import io.tofhir.common.util.CustomMappingFunctionsFactory
import io.tofhir.engine.ToFhirEngine
import io.tofhir.engine.config.ErrorHandlingType.ErrorHandlingType
import io.tofhir.engine.config.ToFhirConfig
import io.tofhir.engine.mapping.{FhirMappingJobManager, MappingContextLoader}
import io.tofhir.engine.model._
import io.tofhir.engine.util.FhirMappingJobFormatter.formats
import io.tofhir.engine.util.FileUtils
import io.tofhir.engine.util.FileUtils.FileExtensions
import io.tofhir.rxnorm.RxNormApiFunctionLibraryFactory
import io.tofhir.server.config.SparkConfig
import io.tofhir.server.model.{BadRequest, ExecuteJobTask, ResourceNotFound, TestResourceCreationRequest}
import io.tofhir.server.service.job.IJobRepository
import io.tofhir.server.service.mapping.IMappingRepository
import io.tofhir.server.service.schema.ISchemaRepository
import io.tofhir.server.util.DataFrameUtil
import org.apache.commons.io
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoders, Row}
import org.json4s.JsonAST.{JBool, JObject, JValue}
import org.json4s.JsonDSL.jobject2assoc
import org.json4s.jackson.JsonMethods
import org.json4s.{JArray, JString}

import java.io.File
import java.util.UUID
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

  val externalMappingFunctions: Map[String, IFhirPathFunctionLibraryFactory] = Map(
    "rxn" -> new RxNormApiFunctionLibraryFactory("https://rxnav.nlm.nih.gov", 2),
    "cst" -> new CustomMappingFunctionsFactory()
  )
  val toFhirEngine = new ToFhirEngine(Some(mappingRepository), Some(schemaRepository), externalMappingFunctions)


  /**
   * Run the job for the given execute job tasks
   *
   * @param projectId      project id the job belongs to
   * @param jobId          job id
   * @param executeJobTask execute job task instance contains mapping urls and error handling type
   * @return
   */
  def runJob(projectId: String, jobId: String, executionId: Option[String], executeJobTask: Option[ExecuteJobTask]): Future[Unit] = {
    if (!jobRepository.getCachedMappingsJobs.contains(projectId) || !jobRepository.getCachedMappingsJobs(projectId).contains(jobId)) {
      throw ResourceNotFound("Mapping job does not exists.", s"A mapping job with id $jobId does not exists in the mapping job repository")
    }

    val mappingJob: FhirMappingJob = jobRepository.getCachedMappingsJobs(projectId)(jobId)

    // get the list of mapping task to be executed
    val mappingTasks = executeJobTask.flatMap(_.mappingUrls) match {
      case Some(urls) => urls.flatMap(url => mappingJob.mappings.find(p => p.mappingRef.contentEquals(url)))
      case None => mappingJob.mappings
    }

    if(mappingTasks.isEmpty) {
      throw BadRequest("No mapping task to execute!", "No mapping task to execute!")
    }

    // all the mappings in mappingTasks should not running, if any of them is running, give already running response
    val JobExecutionMap = toFhirEngine.runningJobRegistry.getRunningExecutions()
    val jobExecution = JobExecutionMap.get(jobId)
    if (jobExecution.isDefined) {
      val runningMappingUrls = jobExecution.get.flatMap(_._2)
      val runningMappingUrlsSet = runningMappingUrls.toSet
      val mappingUrlsSet = mappingTasks.map(_.mappingRef).toSet
      val intersection = runningMappingUrlsSet.intersect(mappingUrlsSet)
      if (intersection.nonEmpty) {
        // create jvalue json response with already running mapping urls as list string in values and execution ids in keys
        // find execution ids for the intersection mapping urls
        val executionIds = jobExecution.get
          .filter(p => p._2.exists(mappingUrl => intersection.contains(mappingUrl)))
          .map(x => {
            (x._1, x._2.filter(mappingUrl => intersection.contains(mappingUrl)))
          })
        // convert execution ids to json response
        val jValueResponse = JArray(executionIds.map(x => {
          JObject(
            "executionId" -> JString(x._1),
            "mappingUrls" -> JArray(x._2.map(JString(_)).toList)
          )
        }).toList)
        // use it in the response message
        throw BadRequest("Mapping tasks are already running!", JsonMethods.compact(JsonMethods.render(jValueResponse)))
      }
    }

    // create execution
    val mappingJobExecution = FhirMappingJobExecution(executionId.getOrElse(UUID.randomUUID().toString), job = mappingJob, projectId = projectId, mappingTasks = mappingTasks,
      mappingErrorHandling = executeJobTask.flatMap(_.mappingErrorHandling).getOrElse(mappingJob.dataProcessingSettings.mappingErrorHandling))
    val fhirMappingJobManager = getFhirMappingJobManager(mappingJob.dataProcessingSettings.mappingErrorHandling)

    // Streaming jobs
    val submittedJob = Future {
      if (mappingJob.sourceSettings.exists(_._2.asStream)) {
        // Delete checkpoint directory if set accordingly
        if (executeJobTask.exists(_.clearCheckpoints)) {
          mappingTasks.foreach(mapping => {
            io.FileUtils.deleteDirectory(new File(mappingJobExecution.getCheckpointDirectory(mapping.mappingRef)))
          })
        }

        fhirMappingJobManager
          .startMappingJobStream(
            mappingJobExecution,
            sourceSettings = mappingJob.sourceSettings,
            sinkSettings = mappingJob.sinkSettings,
            terminologyServiceSettings = mappingJob.terminologyServiceSettings,
            identityServiceSettings = mappingJob.getIdentityServiceSettings()
          )
          .foreach(sq => toFhirEngine.runningJobRegistry.registerStreamingQuery(mappingJobExecution.job.id, mappingJobExecution.id, sq._1, sq._2))
      }

      // Batch jobs
      else {
        val executionFuture: Future[Unit] = fhirMappingJobManager
          .executeMappingJob(
            mappingJobExecution = mappingJobExecution,
            sourceSettings = mappingJob.sourceSettings,
            sinkSettings = mappingJob.sinkSettings,
            terminologyServiceSettings = mappingJob.terminologyServiceSettings,
            identityServiceSettings = mappingJob.getIdentityServiceSettings()
          )

        // Register the job to the registry
        toFhirEngine.runningJobRegistry.registerBatchJob(
          mappingJobExecution.job.id,
          mappingJobExecution.id,
          mappingTasks.map(_.mappingRef),
          executionFuture,
          s"Spark job for job: ${mappingJobExecution.job.id} mappings: ${mappingTasks.map(_.mappingRef).mkString(" ")}"
        )
      }
    }
    logger.debug(s"Submitted job for project: $projectId, job: $jobId, execution: ${mappingJobExecution.id}")
    submittedJob
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

    val fhirMappingJobManager = getFhirMappingJobManager(mappingJob.dataProcessingSettings.mappingErrorHandling)
    val (fhirMapping, dataSourceSettings, dataFrame) = fhirMappingJobManager.readJoinSourceData(mappingTask, mappingJob.sourceSettings, jobId = Some(jobId))
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
        // Get mapping tasks logs for the given execution. ProjectId field is not null for selecting mappingTasksLogs, filter out row error logs.
        val mappingTasksLogs = dataFrame.filter(s"executionId = '$executionId' and projectId is not null")

        // Handle the case where the job has not been run yet, which makes the data frame empty
        if (mappingTasksLogs.isEmpty) {
          Seq.empty
        } else {
          // Collect mapping tasks logs for matching with mappingUrl field of row error logs
          var mappingTasksLogsData = mappingTasksLogs.collect()

          // Get row error logs for the given execution. ProjectId field is null for selecting row error logs, filter out mappingTasksLogs.
          var rowErrorLogs = dataFrame.filter(s"executionId = '$executionId' and projectId is null")

          // Check whether there is any row error
          if(!rowErrorLogs.isEmpty){

            // Select needed columns from row error logs
            rowErrorLogs = rowErrorLogs.select(List("errorCode", "errorDesc", "message", "mappingUrl").map(col):_*)

            // Group row error logs by mapping url
            val rowErrorLogsGroupedByMappingUrl = rowErrorLogs.groupByKey(row => row.get(row.fieldIndex("mappingUrl")).toString)(Encoders.STRING)

            // Add row error details to mapping tasks logs if any error occurred while executing the mapping task.
            val mappingTasksErrorLogsWithRowErrorLogs = rowErrorLogsGroupedByMappingUrl.mapGroups((mappingUrl, rowError) => {
              // Find the related mapping task log to given mapping url
              val mappingTaskLog = mappingTasksLogsData.filter(row => row.getAs[String]("mappingUrl") == mappingUrl)
              // Append row error logs to the related mapping task log
              Row.fromSeq(Row.unapplySeq(mappingTaskLog.head).get :+ rowError.toSeq)
            })(
              // Define a new schema for the resulting rows and create an encoder for it. We will add a "error_logs" column to mapping tasks logs that contains related error logs.
              RowEncoder(mappingTasksLogs.schema.add("error_logs", ArrayType(
                new StructType()
                  .add("errorCode", StringType)
                  .add("errorDesc", StringType)
                  .add("message", StringType)
                  .add("mappingUrl", StringType)
              ))
              )
            )

            // Build a map for updated mapping tasks logs (mappingUrl -> mapping logs with errors)
            val updatedMappingTasksLogsMap = mappingTasksErrorLogsWithRowErrorLogs.collect().map(mappingLogsWithErrors =>
              (mappingLogsWithErrors.getAs[String]("mappingUrl"), mappingLogsWithErrors.getAs[String]("@timestamp")) -> mappingLogsWithErrors
            ).toMap

            // Replace mapping task logs if it is in the map
            mappingTasksLogsData = mappingTasksLogsData.map(mappingTaskLog =>
              updatedMappingTasksLogsMap.getOrElse((mappingTaskLog.getAs[String]("mappingUrl"), mappingTaskLog.getAs[String]("@timestamp")), mappingTaskLog))

          }

          // return json objects for mapping tasks logs
          mappingTasksLogsData.map(row => {
            val rowJson: JObject = JsonMethods.parse(row.json).asInstanceOf[JObject]
            // Add runningStatus field to the json object. Running status is set to true if the execution id is contained in the job executions
            JObject(
              rowJson.obj :+ ("runningStatus" -> JBool(
                toFhirEngine.runningJobRegistry.executionExists((rowJson \ "jobId").extract[String], executionId, (rowJson \ "mappingUrl").extractOpt[String])
              ))
            )
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
              val rows: Seq[Row] = values.toSeq
              // Extract values from the "mappingUrl" column using direct attribute access
              val mappingUrls = rows.map(_.getAs[String]("mappingUrl")).distinct
              // use the timestamp of first one, which is ran first, as timestamp of execution
              val timestamp = rows.head.get(rows.head.fieldIndex("@timestamp")).toString
              // set the status of execution
              var status = "STARTED"
              // Check if there is a row with result other than STARTED
              if (!rows.forall(r => r.get(r.fieldIndex("result")).toString.contentEquals("STARTED"))) {
                val successCount = rows.count(r => r.get(r.fieldIndex("result")).toString.contentEquals("SUCCESS"))
                status = "SUCCESS"
                if (successCount == 0) {
                  status = "FAILURE"
                } else if (successCount != mappingUrls.length) {
                  status = "PARTIAL_SUCCESS"
                }
              }

              Row.fromSeq(Seq(key, mappingUrls, timestamp, status))
            })(RowEncoder(StructType(
              StructField("id", StringType) ::
                StructField("mappingUrls", ArrayType(StringType)) ::
                StructField("startTime", StringType) ::
                StructField("errorStatus", StringType) :: Nil
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
              val paginatedLogs = executionLogs.sort(executionLogs.col("startTime").desc).collect().slice(start, end)

              // Retrieve the running executions for the given job
              val jobExecutions: Set[String] = toFhirEngine.runningJobRegistry.getRunningExecutions(jobId)
              (paginatedLogs.map(row => {
                val rowJson: JObject = JsonMethods.parse(row.json).asInstanceOf[JObject]
                // Add runningStatus field to the json object. Running status is set to true if the execution id is contained in the job executions
                JObject(
                  rowJson.obj :+ ("runningStatus" -> JBool(jobExecutions.contains((rowJson \ "id").extract[String])))
                )
              }), total.toInt)
            }
          }
        }
      case None => throw ResourceNotFound("Mapping job does not exists.", s"A mapping job with id $jobId does not exists")
    }
  }

  /**
   * Returns the execution logs for a specific execution ID.
   *
   * @param projectId    project id the job belongs to
   * @param jobId        job id
   * @param executionId  execution id
   * @return the execution summary as a JSON object
   */
  def getExecutionById(projectId: String, jobId: String, executionId: String): Future[JObject] = {
    // Retrieve the job to validate its existence
    jobRepository.getJob(projectId, jobId).flatMap {
      case Some(_) =>
        // Read logs/tofhir-mappings.log file
        val dataFrame = SparkConfig.sparkSession.read.json("logs/tofhir-mappings.log")
        // Filter logs by job and execution ID
        val filteredLogs = dataFrame.filter(s"jobId = '$jobId' and projectId = '$projectId' and executionId = '$executionId'")
        // Check if any logs exist for the given execution
        if (filteredLogs.isEmpty) { // execution not found, return response with 404 status code
          throw ResourceNotFound("Execution does not exists.", s"An execution with id $executionId does not exists.")
        } else {
          // Extract values from the "mappingUrl" column using direct attribute access
          val mappingUrls = filteredLogs.select("mappingUrl").distinct().collect().map(_.getString(0)).toList
          val successCount = filteredLogs.filter(col("result") === "SUCCESS").count()
          // Use the timestamp of the first log as the execution timestamp
          val timestamp = filteredLogs.select("@timestamp").first().getString(0)
          // Determine the status based on the success count
          val status = if (successCount == 0) "FAILURE" else if (successCount != mappingUrls.length) "PARTIAL_SUCCESS" else "SUCCESS"
          // Create a JSON object representing the execution
          val executionJson = JObject(
            "id" -> JString(executionId),
            "mappingUrls" -> JArray(mappingUrls.map(JString)),
            "startTime" -> JString(timestamp),
            "errorStatus" -> JString(status)
          )
          // Add runningStatus field to the JSON object
          val updatedExecutionJson = executionJson ~ ("runningStatus" -> JBool(toFhirEngine.runningJobRegistry.getRunningExecutions(jobId).contains(executionId)))
          Future.successful(updatedExecutionJson)
        }
      case None =>
        throw ResourceNotFound("Mapping job does not exist.", s"A mapping job with id $jobId does not exist")
    }
  }

  /**
   * Stops the specified job execution. This means that all running executions regarding the mappings included in this job will be stopped.
   *
   * @param jobId Identifier of the job
   * @return
   */
  def stopJobExecution(jobId: String, executionId: String): Future[Unit] = {
    Future {
      if (toFhirEngine.runningJobRegistry.executionExists(jobId, executionId, None)) {
        toFhirEngine.runningJobRegistry.stopJobExecution(jobId, executionId)
        logger.debug(s"Job execution stopped. jobId: $jobId, execution: $executionId")
      } else {
        throw ResourceNotFound("Job execution does not exists.", s"A job execution with jobId: $jobId, executionId: $executionId does not exists.")
      }

    }
  }

  /**
   * Stops the execution of the specific mapping task
   *
   * @param executionId Execution in which the mapping is run
   * @param mappingUrl  Mapping to be stopped
   * @return
   */
  def stopMappingExecution(jobId: String, executionId: String, mappingUrl: String): Future[Unit] = {
    Future {
      if (toFhirEngine.runningJobRegistry.executionExists(jobId, executionId, Some(mappingUrl))) {
        toFhirEngine.runningJobRegistry.stopMappingExecution(jobId, executionId, mappingUrl)
        logger.debug(s"Mapping execution stopped. jobId: $jobId, executionId: $executionId, mappingUrl: $mappingUrl")
      } else {
        throw ResourceNotFound("Mapping execution does not exists.", s"A mapping execution with jobId: $jobId, executionId: $executionId, mappingUrl: $mappingUrl does not exists.")
      }
    }
  }

  private def getFhirMappingJobManager(mappingErrorHandlingType: ErrorHandlingType) =
    new FhirMappingJobManager(
      toFhirEngine.mappingRepo,
      toFhirEngine.contextLoader,
      toFhirEngine.schemaLoader,
      toFhirEngine.functionLibraries,
      toFhirEngine.sparkSession,
      mappingErrorHandlingType,
      toFhirEngine.runningJobRegistry
    )
}
