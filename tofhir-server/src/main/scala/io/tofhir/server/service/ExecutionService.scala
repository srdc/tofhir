package io.tofhir.server.service

import com.typesafe.scalalogging.LazyLogging
import io.onfhir.path.IFhirPathFunctionLibraryFactory
import io.tofhir.common.util.CustomMappingFunctionsFactory
import io.tofhir.engine.config.ToFhirConfig
import io.tofhir.engine.mapping.context.MappingContextLoader
import io.tofhir.engine.mapping.job.FhirMappingJobManager
import io.tofhir.engine.model._
import io.tofhir.engine.util.FhirMappingJobFormatter.formats
import io.tofhir.engine.util.FileUtils
import io.tofhir.engine.util.FileUtils.FileExtensions
import io.tofhir.engine.{Execution, ToFhirEngine}
import io.tofhir.rxnorm.RxNormApiFunctionLibraryFactory
import io.tofhir.server.common.model.{BadRequest, ResourceNotFound}
import io.tofhir.server.model.{ExecuteJobTask, TestResourceCreationRequest}
import io.tofhir.server.repository.job.IJobRepository
import io.tofhir.server.repository.mapping.IMappingRepository
import io.tofhir.server.repository.schema.ISchemaRepository
import io.tofhir.server.util.DataFrameUtil
import io.tofhir.engine.mapping.job.MappingJobScheduler
import org.apache.commons.io
import org.json4s.JsonAST.{JBool, JObject, JValue}
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
 */
class ExecutionService(jobRepository: IJobRepository, mappingRepository: IMappingRepository, schemaRepository: ISchemaRepository) extends LazyLogging {

  val externalMappingFunctions: Map[String, IFhirPathFunctionLibraryFactory] = Map(
    "rxn" -> new RxNormApiFunctionLibraryFactory("https://rxnav.nlm.nih.gov", 2),
    "cst" -> new CustomMappingFunctionsFactory()
  )
  // TODO do not define engine and client as a global variable inside the class. (Testing becomes impossible)
  val toFhirEngine = new ToFhirEngine(Some(mappingRepository), Some(schemaRepository), externalMappingFunctions)

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
      case Some(urls) => urls.flatMap(url => mappingJob.mappings.filter(p => p.mappingRef.contentEquals(url)))
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

    // create an instance of MappingJobScheduler
    val mappingJobScheduler: MappingJobScheduler = MappingJobScheduler.instance(ToFhirConfig.engineConfig.toFhirDbFolderPath)

    // create execution
    val mappingJobExecution = FhirMappingJobExecution(executionId.getOrElse(UUID.randomUUID().toString), job = mappingJob,
      projectId = projectId, mappingTasks = mappingTasks)
    val fhirMappingJobManager = new FhirMappingJobManager(
      toFhirEngine.mappingRepo,
      toFhirEngine.contextLoader,
      toFhirEngine.schemaLoader,
      toFhirEngine.functionLibraries,
      toFhirEngine.sparkSession,
      Some(mappingJobScheduler)
    )

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
            logger.debug(s"Deleted checkpoint directory for jobId: ${mappingJobExecution.jobId}, executionId: ${mappingJobExecution.id}, mappingUrl: ${mapping.mappingRef}, path: ${checkpointDirectory.getAbsolutePath}")
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
        // run the mapping job with scheduler
        if (mappingJob.schedulingSettings.nonEmpty) {
          // check whether the job execution is already scheduled
          if (executionId.nonEmpty && toFhirEngine.runningJobRegistry.isScheduled(mappingJob.id, executionId.get)) {
            throw BadRequest("The mapping job execution is already scheduled!", s"The mapping job execution is already scheduled!")
          }
          // schedule the mapping job
          fhirMappingJobManager
            .scheduleMappingJob(
              mappingJobExecution = mappingJobExecution,
              sourceSettings = mappingJob.sourceSettings,
              sinkSettings = mappingJob.sinkSettings,
              schedulingSettings = mappingJob.schedulingSettings.get,
              terminologyServiceSettings = mappingJob.terminologyServiceSettings,
              identityServiceSettings = mappingJob.getIdentityServiceSettings()
            )
          // start scheduler
          mappingJobScheduler.scheduler.start()
          // register the job to the registry
          toFhirEngine.runningJobRegistry.registerSchedulingJob(mappingJobExecution, mappingJobScheduler.scheduler)
        }

        // run the batch job without scheduling
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
            Some(executionFuture),
            s"Spark job for job: ${mappingJobExecution.jobId} mappings: ${mappingTasks.map(_.mappingRef).mkString(" ")}"
          )
        }
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

    val fhirMappingJobManager = new FhirMappingJobManager(
      toFhirEngine.mappingRepo,
      toFhirEngine.contextLoader,
      toFhirEngine.schemaLoader,
      toFhirEngine.functionLibraries,
      toFhirEngine.sparkSession
    )
    val (fhirMapping, mappingJobSourceSettings, dataFrame) = fhirMappingJobManager.readJoinSourceData(mappingTask, mappingJob.sourceSettings, jobId = Some(jobId))
    val selected = DataFrameUtil.applyResourceFilter(dataFrame, testResourceCreationRequest.resourceFilter)
    fhirMappingJobManager.executeTask(mappingJob.id, fhirMapping, selected, mappingJobSourceSettings, mappingJob.terminologyServiceSettings, mappingJob.getIdentityServiceSettings(), projectId = Some(projectId))
      .map { dataFrame =>
        dataFrame
          .collect() // Collect into an Array[String]
          .toSeq // Convert to Seq[Resource]
      }
  }

  /**
   * Returns the ongoing (i.e. running or scheduled) mapping job executions.
   *
   * @param projectId project id the job belongs to
   * @param jobId     job id
   * @return a list of JSONs indicating the execution id and its status i.e. runningStatus or scheduled
   * @throws ResourceNotFound when mapping job does not exist
   */
  def getExecutions(projectId: String, jobId: String): Future[Seq[JValue]] = {
    // retrieve the job to validate its existence
    jobRepository.getJob(projectId, jobId).flatMap {
      case Some(_) =>
        // Retrieve the running executions for the given job
        val runningExecutionsJson: Seq[JValue] = toFhirEngine.runningJobRegistry.getRunningExecutions(jobId)
          .map(id => JObject(
            List(
              "id" -> JString(id),
              "runningStatus" -> JBool(true)
            )
          )).toSeq
        // Retrieve the scheduled executions for the given job
        val scheduledExecutionsJson: Seq[JValue] = toFhirEngine.runningJobRegistry.getScheduledExecutions(jobId)
          .map(id => JObject(
            List(
              "id" -> JString(id),
              "scheduled" -> JBool(true)
            )
          )).toSeq

        Future.successful(runningExecutionsJson ++ scheduledExecutionsJson)

      case None => throw ResourceNotFound("Mapping job does not exists.", s"A mapping job with id $jobId does not exists")
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
   * Deschedules a job execution.
   *
   * @param jobId       The ID of the job.
   * @param executionId The ID of the job execution.
   * @return A Future[Unit] representing the descheduling process.
   * @throws ResourceNotFound if the specified job execution is not scheduled.
   */
  def descheduleJobExecution(jobId: String, executionId: String): Future[Unit] = {
    Future {
      if (toFhirEngine.runningJobRegistry.isScheduled(jobId, executionId)) {
        toFhirEngine.runningJobRegistry.descheduleJobExecution(jobId, executionId)
        logger.debug(s"Job execution descheduled. jobId: $jobId, execution: $executionId")
      } else {
        throw ResourceNotFound("Job is not scheduled.", s"There is no scheduled job execution with jobId: $jobId, executionId: $executionId.")
      }
    }
  }

  /**
   * Stops all executions associated with a mapping job specified by the jobId.
   *
   * @param jobId The identifier of the mapping job for which executions should be stopped.
   * @return A Future[Unit] representing the completion of the stop operations.
   */
  def stopJobExecutions(jobId: String): Future[Unit] = {
    Future {
      toFhirEngine.runningJobRegistry.stopJobExecutions(jobId)
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

  /**
   * Checks whether a mapping job with the specified jobId is currently running.
   *
   * @param jobId The identifier of the mapping job to be checked for running status.
   * @return A Future[Boolean] indicating whether the specified mapping job is running (true) or not (false).
   */
  def isJobRunning(jobId: String): Future[Boolean] = {
    Future {
      toFhirEngine.runningJobRegistry.isJobRunning(jobId)
    }
  }
}


