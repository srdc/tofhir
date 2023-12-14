package io.tofhir.server.service

import com.typesafe.scalalogging.LazyLogging
import io.onfhir.path.IFhirPathFunctionLibraryFactory
import io.tofhir.common.util.CustomMappingFunctionsFactory
import io.tofhir.engine.config.ToFhirConfig
import io.tofhir.engine.mapping.{FhirMappingJobManager, MappingContextLoader}
import io.tofhir.engine.model._
import io.tofhir.engine.util.FhirMappingJobFormatter.formats
import io.tofhir.engine.util.FileUtils
import io.tofhir.engine.util.FileUtils.FileExtensions
import io.tofhir.engine.{Execution, ToFhirEngine}
import io.tofhir.rxnorm.RxNormApiFunctionLibraryFactory
import io.tofhir.server.model.{BadRequest, ExecuteJobTask, ResourceNotFound, TestResourceCreationRequest}
import io.tofhir.server.service.job.IJobRepository
import io.tofhir.server.service.mapping.IMappingRepository
import io.tofhir.server.service.schema.ISchemaRepository
import io.tofhir.server.util.{DataFrameUtil, LogServiceClient}
import org.apache.commons.io
import org.json4s.JsonAST.{JBool, JObject, JValue}
import org.json4s.JsonDSL.jobject2assoc
import org.json4s.jackson.JsonMethods
import org.json4s.{JArray, JString}

import java.io.File
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

/**
 * Service to handle all execution related operations
 * E.g. Run a mapping job, run a mapping task, run a test resource creation, get execution logs
 *
 * @param jobRepository
 * @param mappingRepository
 * @param schemaRepository
 * @param logServiceEndpoint
 */
class ExecutionService(jobRepository: IJobRepository, mappingRepository: IMappingRepository, schemaRepository: ISchemaRepository, logServiceEndpoint: String) extends LazyLogging {

  val externalMappingFunctions: Map[String, IFhirPathFunctionLibraryFactory] = Map(
    "rxn" -> new RxNormApiFunctionLibraryFactory("https://rxnav.nlm.nih.gov", 2),
    "cst" -> new CustomMappingFunctionsFactory()
  )
  // TODO do not define engine and client as a global variable inside the class. (Testing becomes impossible)
  val toFhirEngine = new ToFhirEngine(Some(mappingRepository), Some(schemaRepository), externalMappingFunctions)
  val logServiceClient = new LogServiceClient(logServiceEndpoint)

  import Execution.actorSystem
  implicit val ec: ExecutionContext = actorSystem.dispatcher

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

    if (mappingTasks.isEmpty) {
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
    val mappingJobExecution = FhirMappingJobExecution(executionId.getOrElse(UUID.randomUUID().toString), job = mappingJob,
      projectId = projectId, mappingTasks = mappingTasks)
    val fhirMappingJobManager = getFhirMappingJobManager

    // Streaming jobs
    val submittedJob = Future {
      if (mappingJob.sourceSettings.exists(_._2.asStream)) {
        // Delete checkpoint directory if set accordingly
        if (executeJobTask.exists(_.clearCheckpoints)) {
          mappingTasks.foreach(mapping => {
            // Reset the archiving offset so that the archiving starts from scratch
            toFhirEngine.fileStreamInputArchiver.resetOffset(mappingJobExecution, mapping.mappingRef)

            val checkpointDirectory: File = new File(mappingJobExecution.getCheckpointDirectory(mapping.mappingRef))
            io.FileUtils.deleteDirectory(checkpointDirectory)
            logger.debug(s"Deleted checkpoint directory for jobId: ${mappingJobExecution.job.id}, executionId: ${mappingJobExecution.id}, mappingUrl: ${mapping.mappingRef}, path: ${checkpointDirectory.getAbsolutePath}")
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
          .foreach(sq => toFhirEngine.runningJobRegistry.registerStreamingQuery(mappingJobExecution, sq._1, sq._2))
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
          mappingJobExecution,
          executionFuture,
          s"Spark job for job: ${mappingJobExecution.job.id} mappings: ${mappingTasks.map(_.mappingRef).mkString(" ")}"
        )
      }
    }
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

    val fhirMappingJobManager = getFhirMappingJobManager
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
  def getExecutionLogs(projectId: String, jobId: String, executionId: String): Future[Seq[JValue]] = {
    logServiceClient.getExecutionLogs(projectId, jobId, executionId)
      .map(mappingTasksLogsResponse => {
        logger.debug(s"Retrieved execution logs for projectId: $projectId, jobId: $jobId, executionId: $executionId")
        mappingTasksLogsResponse.map(logResponse => {
          val logResponseObject: JObject = logResponse.asInstanceOf[JObject]

          JObject(
            logResponseObject.obj :+ ("runningStatus" -> JBool(
              toFhirEngine.runningJobRegistry.executionExists((logResponseObject \ "jobId").extract[String], executionId, (logResponseObject \ "mappingUrl").extractOpt[String])
            )))
        })
      })
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
    jobRepository.getJob(projectId, jobId).flatMap {
      case Some(_) =>
        val page = queryParams.getOrElse("page", "1").toInt
        logServiceClient.getExecutions(projectId, jobId, page).map(paginatedLogsResponse => {
          logger.debug(s"Retrieved executions for projectId: $projectId, jobId: $jobId, page: $page")
          // Retrieve the running executions for the given job
          val jobExecutions: Set[String] = toFhirEngine.runningJobRegistry.getRunningExecutions(jobId)

          val ret = paginatedLogsResponse._1.map(log => {
            val logJson: JObject = log.asInstanceOf[JObject]
            JObject(
              logJson.obj :+ ("runningStatus" -> JBool(jobExecutions.contains((logJson \ "id").extract[String])))
            )
          })
          (ret, paginatedLogsResponse._2)
        })

      case None => throw ResourceNotFound("Mapping job does not exists.", s"A mapping job with id $jobId does not exists")
    }
  }

  /**
   * Returns the execution logs for a specific execution ID.
   *
   * @param projectId   project id the job belongs to
   * @param jobId       job id
   * @param executionId execution id
   * @return the execution summary as a JSON object
   */
  def getExecutionById(projectId: String, jobId: String, executionId: String): Future[JObject] = {
    // Retrieve the job to validate its existence
    jobRepository.getJob(projectId, jobId).flatMap {
      case Some(_) =>
        // Get
        logServiceClient.getExecutionById(projectId, jobId, executionId).map(executionJson=> {
          logger.debug(s"Retrieved execution for projectId: $projectId, jobId: $jobId, executionId: $executionId")
          // Add runningStatus field to the JSON object
          executionJson ~ ("runningStatus" -> JBool(toFhirEngine.runningJobRegistry.getRunningExecutions(jobId).contains(executionId)))
        })
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

  private def getFhirMappingJobManager =
    new FhirMappingJobManager(
      toFhirEngine.mappingRepo,
      toFhirEngine.contextLoader,
      toFhirEngine.schemaLoader,
      toFhirEngine.functionLibraries,
      toFhirEngine.sparkSession,
      toFhirEngine.runningJobRegistry
    )
}


