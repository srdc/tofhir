package io.tofhir.server.service

import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.config.ToFhirConfig
import io.tofhir.engine.env.EnvironmentVariableResolver
import io.tofhir.engine.mapping.context.MappingContextLoader
import io.tofhir.engine.mapping.job.{FhirMappingJobManager, MappingJobScheduler}
import io.tofhir.engine.model._
import io.tofhir.engine.util.FhirMappingJobFormatter.formats
import io.tofhir.engine.util.FileUtils
import io.tofhir.engine.util.FileUtils.FileExtensions
import io.tofhir.engine.{Execution, ToFhirEngine}
import io.tofhir.server.common.model.{BadRequest, ResourceNotFound}
import io.tofhir.server.model.{ExecuteJobTask, TestResourceCreationRequest}
import io.tofhir.server.repository.job.IJobRepository
import io.tofhir.server.repository.mapping.IMappingRepository
import io.tofhir.server.repository.schema.ISchemaRepository
import io.tofhir.server.util.DataFrameUtil
import org.apache.commons.io
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, KeyValueGroupedDataset}
import org.json4s.jackson.JsonMethods
import org.json4s.{JArray, JBool, JObject, JString, JValue}

import java.io.File
import java.util.UUID
import scala.concurrent.{ExecutionContext, ExecutionException, Future}

/**
 * Service to handle all execution related operations
 * E.g. Run a mapping job, run a mapping task, run a test resource creation, get execution logs
 *
 * @param jobRepository
 * @param mappingRepository
 * @param schemaRepository
 */
class ExecutionService(jobRepository: IJobRepository, mappingRepository: IMappingRepository, schemaRepository: ISchemaRepository) extends LazyLogging {

  // TODO do not define engine and client as a global variable inside the class. (Testing becomes impossible)
  val toFhirEngine = new ToFhirEngine(Some(mappingRepository), Some(schemaRepository))

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
    jobRepository.getJob(projectId, jobId) map {
      case None => throw ResourceNotFound("Mapping job does not exists.", s"A mapping job with id $jobId does not exists in the mapping job repository")
      case Some(mj) =>
        val mappingJob = EnvironmentVariableResolver.resolveFhirMappingJob(mj)

        // get the list of mapping task to be executed
        val mappingTasks = executeJobTask.flatMap(_.mappingTaskNames) match {
          case Some(names) => names.flatMap(name => mappingJob.mappings.find(p => p.name.contentEquals(name)))
          case None => mappingJob.mappings
        }

        if (mappingTasks.isEmpty) {
          throw BadRequest("No mapping task to execute!", "No mapping task to execute!")
        }

        // all the mappings in mappingTasks should not running, if any of them is running, give already running response
        val JobExecutionMap = toFhirEngine.runningJobRegistry.getRunningExecutions()
        val jobExecution = JobExecutionMap.get(jobId)
        if (jobExecution.isDefined) {
          val runningMappingTaskNames = jobExecution.get.flatMap(_._2)
          val runningMappingTaskNamesSet = runningMappingTaskNames.toSet
          val mappingTaskNamesSet = mappingTasks.map(_.name).toSet
          val intersection = runningMappingTaskNamesSet.intersect(mappingTaskNamesSet)
          if (intersection.nonEmpty) {
            // create jvalue json response with already running mappingTask names as list string in values and execution ids in keys
            // find execution ids for the intersection mappingTask names
            val executionIds = jobExecution.get
              .filter(p => p._2.exists(name => intersection.contains(name)))
              .map(x => {
                (x._1, x._2.filter(name => intersection.contains(name)))
              })
            // convert execution ids to json response
            val jValueResponse = JArray(executionIds.map(x => {
              JObject(
                "executionId" -> JString(x._1),
                "mappingTaskNames" -> JArray(x._2.map(JString(_)).toList)
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
        val submittedJob =
          if (mappingJob.sourceSettings.exists(_._2.asStream)) {
            // Delete checkpoint directory if set accordingly
            if (executeJobTask.exists(_.clearCheckpoints)) {
              mappingTasks.foreach(mappingTask => {
                // Reset the archiving offset so that the archiving starts from scratch
                toFhirEngine.fileStreamInputArchiver.resetOffset(mappingJobExecution, mappingTask.name)

                val checkpointDirectory: File = new File(mappingJobExecution.getCheckpointDirectory(mappingTask.name))
                io.FileUtils.deleteDirectory(checkpointDirectory)
                logger.debug(s"Deleted checkpoint directory for jobId: ${mappingJobExecution.jobId}, executionId: ${mappingJobExecution.id}, mappingTaskName: ${mappingTask.name}, path: ${checkpointDirectory.getAbsolutePath}")
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
                s"Spark job for job: ${mappingJobExecution.jobId} mappingTaskNames: ${mappingTasks.map(_.name).mkString(" ")}"
              )
            }
          }
        submittedJob
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
  def testMappingWithJob(projectId: String, jobId: String, testResourceCreationRequest: TestResourceCreationRequest): Future[Seq[FhirMappingResultsForInput]] = {
    jobRepository.getJob(projectId, jobId) flatMap {
      case None => throw ResourceNotFound("Mapping job does not exists.", s"A mapping job with id $jobId does not exists in the mapping job repository")
      case Some(mj) =>
        val mappingJob = EnvironmentVariableResolver.resolveFhirMappingJob(mj)

        logger.debug(s"Testing the mapping ${testResourceCreationRequest.fhirMappingTask.mappingRef} inside the job $jobId by selecting ${testResourceCreationRequest.resourceFilter.numberOfRows} ${testResourceCreationRequest.resourceFilter.order} records.")

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
        // Define the updated jobSourceSettings where asStream is set to false if the setting is a KafkaSourceSettings
        val jobSourceSettings: Map[String, MappingJobSourceSettings] = mappingJob.sourceSettings.map {
          case (key, kafkaSettings: KafkaSourceSettings) =>
            key -> kafkaSettings.copy(asStream = false) // Copy and update asStream to false for KafkaSourceSettings
          case other => other // Keep other source settings unchanged
        }

        val (fhirMapping, mappingJobSourceSettings, dataFrame) =
          try {
            fhirMappingJobManager.readJoinSourceData(mappingTask, jobSourceSettings, jobId = Some(jobId), isTestExecution = true)
          } catch {
            // Check if the root cause of the exception is an AnalysisException due to a missing file path.
            // If the error class is PATH_NOT_FOUND, extract and clean up the message by removing the prefix,
            // then throw a user-friendly BadRequest error with the relevant details.
            // Otherwise, rethrow the original exception.
            case e: Exception =>
              val PATH_NOT_FOUND = "PATH_NOT_FOUND"
              Option(e.getCause) match {
                case Some(ae: AnalysisException) if ae.getErrorClass.contentEquals(PATH_NOT_FOUND) =>
                  val cleanMessage = ae.getMessage.stripPrefix(s"[$PATH_NOT_FOUND] ").trim
                  throw BadRequest("File Not Found", cleanMessage)
                case _ =>
                  throw e
              }
          }
        val selectedDataFrame = DataFrameUtil.applyResourceFilter(dataFrame, testResourceCreationRequest.resourceFilter)
          .distinct() // Remove duplicate rows to ensure each FHIR Resource is represented only once per source.
        // This prevents confusion for users in the UI, as displaying the same resource multiple times could lead to misunderstandings.
        fhirMappingJobManager.executeTask(mappingJob.id, mappingTask.name, fhirMapping, selectedDataFrame, mappingJobSourceSettings, mappingJob.terminologyServiceSettings, mappingJob.getIdentityServiceSettings(), projectId = Some(projectId))
          .map { resultingDataFrame =>
            // Import implicits for the Spark session
            import toFhirEngine.sparkSession.implicits._
            // Group by the 'source' column
            val grouped: KeyValueGroupedDataset[String, FhirMappingResult] = resultingDataFrame
              .groupByKey((result: FhirMappingResult) => result.source)
            // Map each group to FhirMappingResultForInput
            grouped.mapGroups(FhirMappingResultConverter.convertToFhirMappingResultsForInput).collect().toSeq
          }.recover{
            case ee: ExecutionException =>
              Option(ee.getCause) match {
                // special handling of UnknownTopicOrPartitionException to include the missing topic names
                case Some(_: UnknownTopicOrPartitionException) =>
                  val topicNames: Seq[String] = testResourceCreationRequest.fhirMappingTask.sourceBinding.map(source => source._2.asInstanceOf[KafkaSource].topicName).toSeq
                  throw BadRequest(
                    "Invalid Kafka Topics",
                    s"The following Kafka topic(s) specified in the mapping task do not exist: ${topicNames.mkString(", ")}."
                  )
                case _ =>
                  throw ee
              }
            case se: SparkException =>
              throw BadRequest(
                "Invalid Source File",
                se.getCause.getMessage.replace("\n", " "), Some(se)
              )
            case e: Exception =>
              throw e
          }
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
   * @param executionId     Execution in which the mapping is run
   * @param mappingTaskName Mapping to be stopped
   * @return
   */
  def stopMappingExecution(jobId: String, executionId: String, mappingTaskName: String): Future[Unit] = {
    Future {
      if (toFhirEngine.runningJobRegistry.executionExists(jobId, executionId, Some(mappingTaskName))) {
        toFhirEngine.runningJobRegistry.stopMappingExecution(jobId, executionId, mappingTaskName)
        logger.debug(s"Mapping execution stopped. jobId: $jobId, executionId: $executionId, mappingTaskName: $mappingTaskName")
      } else {
        throw ResourceNotFound("Mapping execution does not exists.", s"A mapping execution with jobId: $jobId, executionId: $executionId, mappingTaskName: $mappingTaskName does not exists.")
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


