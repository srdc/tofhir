package io.tofhir.engine.mapping.job

import com.typesafe.scalalogging.Logger
import io.onfhir.path.IFhirPathFunctionLibraryFactory
import io.tofhir.engine.config.ToFhirConfig
import io.tofhir.engine.data.read.SourceHandler
import io.tofhir.engine.data.write.{BaseFhirWriter, FhirWriterFactory, SinkHandler}
import io.tofhir.engine.execution.log.ExecutionLogger
import io.tofhir.engine.mapping._
import io.tofhir.engine.mapping.context.IMappingContextLoader
import io.tofhir.engine.mapping.schema.IFhirSchemaLoader
import io.tofhir.engine.model._
import io.tofhir.engine.model.exception.{FhirMappingException, FhirMappingJobStoppedException}
import io.tofhir.engine.repository.mapping.IFhirMappingRepository
import it.sauronsoftware.cron4j.SchedulingPattern
import org.apache.spark.SparkThrowable
import org.apache.spark.sql.functions.{collect_list, struct, udf}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.io.{File, FileNotFoundException, FileWriter}
import java.net.URI
import java.time.{Instant, LocalDateTime, ZoneOffset}
import javax.ws.rs.BadRequestException
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.io.Source

/**
 * Main entrypoint for FHIR mapping jobs
 *
 * @param fhirMappingRepository Repository for mapping definitions
 * @param contextLoader         Context loader
 * @param schemaLoader          Schema (StructureDefinition) loader
 * @param spark                 Spark session
 * @param ec                    Execution context
 */
class FhirMappingJobManager(
                             fhirMappingRepository: IFhirMappingRepository,
                             contextLoader: IMappingContextLoader,
                             schemaLoader: IFhirSchemaLoader,
                             functionLibraries: Map[String, IFhirPathFunctionLibraryFactory],
                             spark: SparkSession,
                             mappingJobScheduler: Option[MappingJobScheduler] = Option.empty
                           )(implicit ec: ExecutionContext) extends IFhirMappingJobManager {

  private val logger: Logger = Logger(this.getClass)

  /**
   * Execute the given batch mapping job and write the resulting FHIR resources to given sink
   *
   * @param mappingJobExecution        Fhir Mapping Job execution
   * @param sourceSettings             Settings for the source system(s)
   * @param sinkSettings               FHIR sink settings (can be a FHIR repository, file system, kafka)
   * @param terminologyServiceSettings Settings for terminology service to use within mappings (e.g. lookupDisplay)
   * @param identityServiceSettings    Settings for identity service to use within mappings (e.g. resolveIdentifier)
   * @param timeRange                  Time range for source data to map if given
   * @return
   */
  override def executeMappingJob(mappingJobExecution: FhirMappingJobExecution,
                                 sourceSettings: Map[String, DataSourceSettings],
                                 sinkSettings: FhirSinkSettings,
                                 terminologyServiceSettings: Option[TerminologyServiceSettings] = None,
                                 identityServiceSettings: Option[IdentityServiceSettings] = None,
                                 timeRange: Option[(LocalDateTime, LocalDateTime)] = None): Future[Unit] = {
    val fhirWriter = FhirWriterFactory.apply(sinkSettings)
    fhirWriter.validate()
    mappingJobExecution.mappingTasks.foldLeft(Future((): Unit)) { (f, task) => // Initial empty Future
      f.flatMap { _ => // Execute the Futures in the Sequence consecutively (not in parallel)
        // log the start of the FHIR mapping task execution
        ExecutionLogger.logExecutionStatus(mappingJobExecution, FhirMappingJobResult.STARTED ,Some(task.mappingRef))
        readSourceExecuteAndWriteInBatches(mappingJobExecution.copy(mappingTasks = Seq(task)), sourceSettings,
          fhirWriter, terminologyServiceSettings, identityServiceSettings, timeRange)
      }.recover {
        // Check whether the job is stopped
        case se: SparkThrowable if se.getMessage.contains("cancelled part of cancelled job group") =>
          // log the execution status as "STOPPED"
          ExecutionLogger.logExecutionStatus(mappingJobExecution, FhirMappingJobResult.STOPPED, Some(task.mappingRef))
          throw FhirMappingJobStoppedException(s"Execution '${mappingJobExecution.id}' of job '${mappingJobExecution.jobId}' in project ${mappingJobExecution.projectId}' terminated manually!")
        // Exceptions from Spark executors are wrapped inside a SparkException, which are caught below
        case se: SparkThrowable =>
          se.getCause match {
            // log the mapping job result and exception for the errors encountered while reading the schema or writing the FHIR Resources
            case _ =>
              // log the execution status as "FAILURE"
              ExecutionLogger.logExecutionStatus(mappingJobExecution, FhirMappingJobResult.FAILURE, Some(task.mappingRef), Some(se))
          }
        // Pass the stop exception to the upstream Futures in the chain laid out by foldLeft above
        case t: FhirMappingJobStoppedException =>
          // log the execution status as "SKIPPED"
          ExecutionLogger.logExecutionStatus(mappingJobExecution, FhirMappingJobResult.SKIPPED, Some(task.mappingRef))
          throw t
        case e: Throwable =>
          // log the execution status as "FAILURE"
          ExecutionLogger.logExecutionStatus(mappingJobExecution, FhirMappingJobResult.FAILURE, Some(task.mappingRef), Some(e))
      }
    } map { _ => logger.debug(s"MappingJob execution finished for MappingJob: ${mappingJobExecution.jobId}.") }
  }

  /**
   * Start streaming mapping job. A future of StreamingQuery is returned for each mapping task included in the job, encapsulated in a map.
   *
   * @param mappingJobExecution        Fhir Mapping Job execution
   * @param sourceSettings             Settings for the source system(s)
   * @param sinkSettings               FHIR sink settings (can be a FHIR repository, file system, kafka)
   * @param terminologyServiceSettings Settings for terminology service to use within mappings (e.g. lookupDisplay)
   * @param identityServiceSettings    Settings for identity service to use within mappings (e.g. resolveIdentifier)
   * @return A map of (mapping url -> streaming query futures).
   */
  override def startMappingJobStream(mappingJobExecution: FhirMappingJobExecution,
                                     sourceSettings: Map[String, DataSourceSettings],
                                     sinkSettings: FhirSinkSettings,
                                     terminologyServiceSettings: Option[TerminologyServiceSettings] = None,
                                     identityServiceSettings: Option[IdentityServiceSettings] = None,
                                    ): Map[String, Future[StreamingQuery]] = {
    val fhirWriter = FhirWriterFactory.apply(sinkSettings)
    fhirWriter.validate()
    mappingJobExecution.mappingTasks
      .map(t => {
        logger.debug(s"Streaming mapping job ${mappingJobExecution.jobId}, mapping url ${t.mappingRef} is started and waiting for the data...")
        // log the start of the FHIR mapping task execution
        ExecutionLogger.logExecutionStatus(mappingJobExecution, FhirMappingJobResult.STARTED ,Some(t.mappingRef))
        // Construct a tuple of (mapping url, Future[StreamingQuery])
        t.mappingRef ->
          readSourceAndExecuteTask(mappingJobExecution.jobId, t, sourceSettings, terminologyServiceSettings, identityServiceSettings, executionId = Some(mappingJobExecution.id), projectId = Some(mappingJobExecution.projectId))
            .map(ts => {
              SinkHandler.writeStream(spark, mappingJobExecution, ts, fhirWriter, t.mappingRef)
            })
            .recover {
              case e: Throwable =>
                // log the execution status as "FAILURE"
                ExecutionLogger.logExecutionStatus(mappingJobExecution, FhirMappingJobResult.FAILURE, Some(t.mappingRef), Some(e))
                throw e
            }
      })
      .toMap
  }

  /**
   * Schedule to execute the given mapping job with given cron expression and write the resulting FHIR resources to the given sink
   *
   * @param mappingJobExecution        Fhir Mapping Job execution
   * @param sourceSettings             Settings for the source system(s)
   * @param sinkSettings               FHIR sink settings (can be a FHIR repository, file system, kafka)
   * @param schedulingSettings         Settings for scheduling the job
   * @param terminologyServiceSettings Settings for terminology service to use within mappings (e.g. lookupDisplay)
   * @param identityServiceSettings    Settings for identity service to use within mappings (e.g. resolveIdentifier)
   * @return
   * @throws BadRequestException when the given cron expression is invalid
   */
  override def scheduleMappingJob(mappingJobExecution: FhirMappingJobExecution,
                                  sourceSettings: Map[String, DataSourceSettings],
                                  sinkSettings: FhirSinkSettings,
                                  schedulingSettings: BaseSchedulingSettings,
                                  terminologyServiceSettings: Option[TerminologyServiceSettings] = None,
                                  identityServiceSettings: Option[IdentityServiceSettings] = None,
                                 ): Unit = {

    if (mappingJobScheduler.isEmpty) {
      throw new IllegalStateException("scheduleMappingJob cannot be called if the FhirMappingJobManager's mappingJobScheduler is not configured.")
    }
    // validate the cron expression
    if (!SchedulingPattern.validate(schedulingSettings.cronExpression)) {
      throw new BadRequestException(s"'${schedulingSettings.cronExpression}' is not a valid cron expression.")
    }
    // find the start time for SQL data sources
    val startTime = schedulingSettings match {
      case SQLSchedulingSettings(_, initialTime) =>
        if (initialTime.isEmpty) {
          logger.info(s"initialTime is not specified in the mappingJob. I will sync all the data from midnight, January 1, 1970 to the next run time.")
          Instant.ofEpochMilli(0L).atOffset(ZoneOffset.UTC).toLocalDateTime
        } else {
          LocalDateTime.parse(initialTime.get)
        }
      case SchedulingSettings(_) =>
        Instant.ofEpochMilli(0L).atOffset(ZoneOffset.UTC).toLocalDateTime
    }
    // Schedule a task
    mappingJobScheduler.get.scheduler.schedule(schedulingSettings.cronExpression, new Runnable() {
      override def run(): Unit = {
        val scheduledJob = runnableMappingJob(mappingJobExecution, startTime, sourceSettings, sinkSettings, terminologyServiceSettings, identityServiceSettings, schedulingSettings)
        Await.result(scheduledJob, Duration.Inf)
      }
    })
  }

  /**
   * Runnable for scheduled periodic mapping job
   *
   * @param mappingJobExecution Mapping job execution
   * @param startTime           Initial start time for source data
   * @param sourceSettings      Settings for the source system
   * @param sinkSettings        FHIR sink settings/configurations
   * @param schedulingSettings  Scheduling information
   * @return
   */
  private def runnableMappingJob(mappingJobExecution: FhirMappingJobExecution,
                                 startTime: LocalDateTime,
                                 sourceSettings: Map[String, DataSourceSettings],
                                 sinkSettings: FhirSinkSettings,
                                 terminologyServiceSettings: Option[TerminologyServiceSettings] = None,
                                 identityServiceSettings: Option[IdentityServiceSettings] = None,
                                 schedulingSettings: BaseSchedulingSettings): Future[Unit] = {
    val timeRange = getScheduledTimeRange(mappingJobExecution.jobId, mappingJobScheduler.get.folderUri, startTime)
    logger.info(s"Running scheduled job with the expression: ${schedulingSettings.cronExpression}")
    logger.info(s"Synchronizing data between ${timeRange._1} and ${timeRange._2}")
    executeMappingJob(mappingJobExecution, sourceSettings, sinkSettings, terminologyServiceSettings, identityServiceSettings, Some(timeRange))
      .map(_ => {
        val writer = new FileWriter(s"${mappingJobScheduler.get.folderUri.getPath}/${mappingJobExecution.jobId}.txt", true)
        try writer.write(timeRange._2.toString + "\n") finally writer.close() //write last sync time to the file
      })
  }

  /**
   * Read the latest synchronization time point for the job
   *
   * @param mappingJobId Job identifier
   * @param folderUri    Folder for sync files
   * @param startTime    Initial start time for the job (for source data)
   * @return
   */
  private def getScheduledTimeRange(mappingJobId: String, folderUri: URI, startTime: LocalDateTime): (LocalDateTime, LocalDateTime) = {
    val file = new File(folderUri)
    if (!file.exists || !file.isDirectory) {
      file.mkdirs()
    }
    try {
      val source = Source.fromFile(s"${folderUri.getPath}/$mappingJobId.txt") //read last sync time from file
      val lines = source.getLines()
      val lastLine = lines.foldLeft("") { case (_, line) => line }
      (LocalDateTime.parse(lastLine), LocalDateTime.now()) //(lastSyncTime, currentTime)}
    } catch {
      case _: FileNotFoundException => (startTime, LocalDateTime.now())
    }
  }

  /**
   * Execute the given mapping task and write the resulting FHIR resources to the given sink
   *
   * @param mappingJobExecution        Fhir Mapping Job execution
   * @param sourceSettings             Settings for the source system(s)
   * @param sinkSettings               FHIR sink settings (can be a FHIR repository, file system, kafka)
   * @param terminologyServiceSettings Settings for terminology service to use within mappings (e.g. lookupDisplay)
   * @param identityServiceSettings    Settings for identity service to use within mappings (e.g. resolveIdentifier)
   * @return
   */
  override def executeMappingTask(mappingJobExecution: FhirMappingJobExecution,
                                  sourceSettings: Map[String, DataSourceSettings],
                                  sinkSettings: FhirSinkSettings,
                                  terminologyServiceSettings: Option[TerminologyServiceSettings] = None,
                                  identityServiceSettings: Option[IdentityServiceSettings] = None,
                                 ): Future[Unit] = {
    val fhirWriter = FhirWriterFactory.apply(sinkSettings)
    fhirWriter.validate()

    readSourceAndExecuteTask(mappingJobExecution.jobId, mappingJobExecution.mappingTasks.head, sourceSettings, terminologyServiceSettings, identityServiceSettings, executionId = Some(mappingJobExecution.id), projectId = Some(mappingJobExecution.projectId))
      .map {
        dataset =>
          SinkHandler.writeBatch(spark, mappingJobExecution, Some(mappingJobExecution.mappingTasks.head.mappingRef), dataset, fhirWriter)
      }
  }

  /**
   * Read all the source data and execute the mapping task
   *
   * @param jobId                      Job identifier
   * @param task                       FHIR Mapping task
   * @param sourceSettings             Source settings
   * @param terminologyServiceSettings Terminology service settings
   * @param identityServiceSettings    Identity service settings
   * @param timeRange                  Time range for the source data to load
   * @param executionId                Id of FhirMappingJobExecution object
   * @param projectId                  Project identifier associated with the mapping job
   * @return
   */
  private def readSourceAndExecuteTask(jobId: String,
                                       task: FhirMappingTask,
                                       sourceSettings: Map[String, DataSourceSettings],
                                       terminologyServiceSettings: Option[TerminologyServiceSettings] = None,
                                       identityServiceSettings: Option[IdentityServiceSettings] = None,
                                       timeRange: Option[(LocalDateTime, LocalDateTime)] = None,
                                       executionId: Option[String] = None,
                                       projectId: Option[String] = None
                                      ): Future[Dataset[FhirMappingResult]] = {
    // Using Future.apply to convert the result of readJoinSourceData into a Future
    // ensuring that if there's an error in readJoinSourceData, it will be propagated as a failed future
    Future.apply(readJoinSourceData(task, sourceSettings, timeRange, jobId = Some(jobId))) flatMap {
      case (fhirMapping, mds, df) => executeTask(jobId, fhirMapping, df, mds, terminologyServiceSettings, identityServiceSettings, executionId, projectId = projectId)
    }
  }

  /**
   * Read the source data, divide it into batches and execute the mapping (first mapping task in the Fhir Mapping Job
   * Execution) and write each batch sequentially
   *
   * @param mappingJobExecution        Fhir Mapping Job execution
   * @param sourceSettings             Source settings
   * @param fhirWriter                 FHIR writer
   * @param terminologyServiceSettings Terminology service settings
   * @param identityServiceSettings    Identity service settings
   * @param timeRange                  Time range for the source data to load
   * @return
   */
  private def readSourceExecuteAndWriteInBatches(mappingJobExecution: FhirMappingJobExecution,
                                                 sourceSettings: Map[String, DataSourceSettings],
                                                 fhirWriter: BaseFhirWriter,
                                                 terminologyServiceSettings: Option[TerminologyServiceSettings] = None,
                                                 identityServiceSettings: Option[IdentityServiceSettings] = None,
                                                 timeRange: Option[(LocalDateTime, LocalDateTime)] = None): Future[Unit] = {
    val mappingTask = mappingJobExecution.mappingTasks.head
    logger.debug(s"Reading source data for mapping ${mappingTask.mappingRef} within mapping job ${mappingJobExecution.jobId} ...")
    val (fhirMapping, mds, df) = readJoinSourceData(mappingTask, sourceSettings, timeRange, jobId = Some(mappingJobExecution.jobId))
    val sizeOfDf: Long = df.count()
    logger.debug(s"$sizeOfDf records read for mapping ${mappingTask.mappingRef} within mapping job ${mappingJobExecution.jobId} ...")

    val result = ToFhirConfig.engineConfig.maxBatchSizeForMappingJobs match {
      //If not specify run it as single batch
      case None =>
        logger.debug(s"Executing the mapping ${mappingTask.mappingRef} within job ${mappingJobExecution.jobId} ...")
        executeTask(mappingJobExecution.jobId, fhirMapping, df, mds, terminologyServiceSettings, identityServiceSettings, Some(mappingJobExecution.id), Some(mappingJobExecution.projectId))
          .map(dataset => SinkHandler.writeBatch(spark, mappingJobExecution, Some(mappingTask.mappingRef), dataset, fhirWriter)) // Write the created FHIR Resources to the FhirWriter
      case Some(batchSize) if sizeOfDf < batchSize =>
        logger.debug(s"Executing the mapping ${mappingTask.mappingRef} within job ${mappingJobExecution.jobId} ...")
        executeTask(mappingJobExecution.jobId, fhirMapping, df, mds, terminologyServiceSettings, identityServiceSettings, Some(mappingJobExecution.id), Some(mappingJobExecution.projectId))
          .map(dataset => SinkHandler.writeBatch(spark, mappingJobExecution, Some(mappingTask.mappingRef), dataset, fhirWriter)) // Write the created FHIR Resources to the FhirWriter
      //Otherwise divide the data into batches
      case Some(batchSize) =>
        val numOfBatch: Int = Math.ceil(sizeOfDf * 1.0 / batchSize * 1.0).toInt
        logger.debug(s"Executing the mapping ${mappingTask.mappingRef} within job ${mappingJobExecution.jobId} in $numOfBatch batches ...")
        val splitDf = df.randomSplit((1 to numOfBatch).map(_ => 1.0).toArray[Double])
        splitDf
          .zipWithIndex
          .foldLeft(Future.apply(())) {
            case (fj, (df, i)) => fj.flatMap(_ =>
              executeTask(mappingJobExecution.jobId, fhirMapping, df, mds, terminologyServiceSettings, identityServiceSettings, Some(mappingJobExecution.id), projectId = Some(mappingJobExecution.projectId))
                .map(dataset => SinkHandler.writeBatch(spark, mappingJobExecution, Some(mappingTask.mappingRef), dataset, fhirWriter))
                .map(_ => logger.debug(s"Batch ${i + 1} is completed for mapping ${mappingTask.mappingRef} within MappingJob: ${mappingJobExecution.jobId}..."))
            )
          }
    }
    result.map(r => {
      // log the result of mapping task execution
      ExecutionLogger.logExecutionResultForBatchMappingTask(mappingJobExecution.id)
      r
    })
  }

  /**
   * Read and join the source data
   *
   * @param task           FHIR Mapping task
   * @param sourceSettings Source settings
   * @param timeRange      Time range for the source data to load
   * @param jobId          The identifier of mapping job which executes the mapping
   */
  def readJoinSourceData(task: FhirMappingTask,
                         sourceSettings: Map[String, DataSourceSettings],
                         timeRange: Option[(LocalDateTime, LocalDateTime)] = None,
                         jobId: Option[String] = None): (FhirMapping, DataSourceSettings, DataFrame) = {
    // if the FhirMapping task includes the mapping to be executed (the case where the mapping is being tested), use it,
    // otherwise retrieve it from the repository
    val mapping = task.mapping match {
      case Some(mapping) => mapping
      case None => fhirMappingRepository.getFhirMappingByUrl(task.mappingRef)
    }
    // remove slice names from the mapping, otherwise FHIR resources will be created with slice names in fields starting with @
    val fhirMapping = mapping.removeSliceNames()
    val sourceNames = fhirMapping.source.map(_.alias).toSet
    val namesForSuppliedSourceContexts = task.sourceContext.keySet
    if (sourceNames != namesForSuppliedSourceContexts)
      throw FhirMappingException(s"Invalid mapping task, source context is not given for some mapping source(s) ${sourceNames.diff(namesForSuppliedSourceContexts).mkString(", ")}")

    //Get the source schemas
    val sources =
      fhirMapping.source.map(s =>
        (
          s.alias, //Alias for the source
          schemaLoader.getSchema(s.url), //URL of the schema for the source
          task.sourceContext(s.alias), //Get source context
          sourceSettings.get(s.alias).orElse(sourceSettings.get("*")).getOrElse(sourceSettings.head._2), //Get source settings from the job definition for that alias or for all aliases (*). If they do not exist, get the first one's settings!!
          timeRange
        ))
    //Read sources into Spark as DataFrame
    val sourceDataFrames =
      sources.map {
        case (alias, schema, sourceContext, sourceStt, timeRange) =>
          alias ->
            SourceHandler.readSource(alias, spark, sourceContext, sourceStt, schema, timeRange, jobId = jobId)
      }

    val df = handleJoin(fhirMapping.source, sourceDataFrames)

    val repartitionedDf = ToFhirConfig.engineConfig.partitionsForMappingJobs match {
      case None => df
      case Some(p) => df.repartition(p)
    }
    //repartitionedDf.printSchema()
    //repartitionedDf.show(100)
    (fhirMapping, sources.head._4, repartitionedDf)
  }

  /**
   * Execute a single mapping task.
   *
   * @param jobId                      Job identifier
   * @param fhirMapping                toFHIR Mapping definition
   * @param df                         Source data to be mapped
   * @param mainSourceSettings         Main source data settings
   * @param terminologyServiceSettings Terminology service settings
   * @param identityServiceSettings    Identity service settings
   * @param executionId                Id of FhirMappingJobExecution object
   * @param projectId                  Project identifier associated with the mapping job
   * @return
   */
  def executeTask(jobId: String,
                  fhirMapping: FhirMapping,
                  df: DataFrame,
                  mainSourceSettings: DataSourceSettings,
                  terminologyServiceSettings: Option[TerminologyServiceSettings] = None,
                  identityServiceSettings: Option[IdentityServiceSettings] = None,
                  executionId: Option[String] = None,
                  projectId: Option[String] = None
                 ): Future[Dataset[FhirMappingResult]] = {
    //Load the contextual data for the mapping
    Future
      .sequence(
        fhirMapping
          .context
          .toSeq
          .map(cdef => contextLoader.retrieveContext(cdef._2).map(context => cdef._1 -> context))
      ).map(loadedContextMap => {
        //Get configuration context
        val configurationContext = mainSourceSettings.toConfigurationContext
        //Construct the mapping service
        val fhirMappingService = new FhirMappingService(jobId, fhirMapping.url, fhirMapping.source.map(_.alias), (loadedContextMap :+ configurationContext).toMap, fhirMapping.mapping, fhirMapping.variable, terminologyServiceSettings, identityServiceSettings, functionLibraries, projectId)
        MappingTaskExecutor.executeMapping(spark, df, fhirMappingService, executionId)
      })
  }

  /**
   * Handle the joining of source data frames
   *
   * @param sources          Defined sources within the mapping
   * @param sourceDataFrames Source data frames loaded
   * @return
   */
  private def handleJoin(sources: Seq[FhirMappingSource], sourceDataFrames: Seq[(String, DataFrame)]): DataFrame = {

    // Rename a DataFrame's column name from dotted version to camelcase since dots have special meaning in Spark's column names.
    def toCamelCase(input: String): String = {
      input.split("\\.").toList match {
        case Nil => ""
        case head :: tail =>
          head + tail.map(_.capitalize).mkString("")
      }
    }

    // For each column which is a FHIR reference, remove FHIR resource name so that the joins can work.
    //  Patient/1234 -> 1234
    def transformFhirReferenceColumns(joinCols: Seq[String], df: DataFrame): DataFrame = {
      // This is a Spark UDF to remove FHIR resource names from values of FHIR references.
      val fhirReferenceResourceNameRemoverUDF = udf((reference: String) => {
        if (reference.matches("^[A-Z].*/.*$")) {
          reference.substring(reference.indexOf('/') + 1)
        } else {
          reference
        }
      })
      joinCols.filter(_.contains(".reference")).foldLeft(df) {
        case (df, refColumn) => df.withColumn(toCamelCase(refColumn), fhirReferenceResourceNameRemoverUDF(df.col(toCamelCase(refColumn))))
      }
    }

    sourceDataFrames match {
      case Seq(_ -> df) => df
      case _ => //If we have multiple sources
        val mainSource = sourceDataFrames.head._1 // We accept the 1st source as the main source and left-join the other sources on this main source.
        val mainJoinOnColumns = sources.find(_.alias == mainSource).get.joinOn

        // Add the JSON object of the whole Row as a column to the DataFrame of the main source
        var mainDf = sourceDataFrames.head._2.withColumn(s"__$mainSource", struct("*"))

        // Find the values addressed by each column (they can be subject.reference or identifier.value (Spark navigates the DataFrame accordingly, like FHIRPath)),
        //  add them to the DataFrame with an alias for each column to convert subject.reference to subjectReference with toCamelCase function.
        // Construct a DataFrame with these join columns and the JSON object of the Row.
        mainDf = mainDf.select(
          mainJoinOnColumns.map(c => mainDf.col(c).as(toCamelCase(c))) :+ mainDf.col(s"__$mainSource"): _*)

        // This is a hack to remove the FHIR resource names from reference fields so that join can work!
        mainDf = transformFhirReferenceColumns(mainJoinOnColumns, mainDf)

        //Group other dataframes on join columns and rename their join columns
        val otherDfs: Seq[(DataFrame, Seq[String])] =
          sourceDataFrames
            .tail // The first source is the main and the rest are others to be left-joined
            .map {
              case (alias, df) =>
                val joinColumnStmts = sources.find(_.alias == alias).get.joinOn
                val colsToJoinOn = joinColumnStmts.filter(_ != null)
                var groupedDf = df
                  .groupBy(colsToJoinOn.map(c => df.col(c).as(toCamelCase(c))): _*)
                  .agg(collect_list(struct("*")).as(s"__$alias"))
                groupedDf = transformFhirReferenceColumns(colsToJoinOn, groupedDf)
                if (colsToJoinOn.toSet.subsetOf(mainJoinOnColumns.toSet))
                  groupedDf -> colsToJoinOn
                else {
                  val actualJoinColumns = joinColumnStmts.map(toCamelCase).zip(mainJoinOnColumns.map(toCamelCase)).filter(_._1 != null)
                  actualJoinColumns
                    .foldLeft(groupedDf) {
                      case (gdf, (c1, r1)) => gdf.withColumnRenamed(c1, r1)
                    } -> actualJoinColumns.map(_._2)
                }
            }
        //Join other data frames to main data frame
        otherDfs.foldLeft(mainDf) {
          case (mdf, odf) => mdf.join(odf._1, odf._2, "left")
        }
    }
  }

  /**
   * Execute the given mapping job and return the resulting FhirMappingResult
   *
   * @param mappingJobExecution        Fhir Mapping Job execution
   * @param sourceSettings             Settings for the source system
   * @param terminologyServiceSettings Settings for terminology service to use within mappings (e.g. lookupDisplay)
   * @param identityServiceSettings    Settings for identity service to use within mappings (e.g. resolveIdentifier)
   * @return
   */
  override def executeMappingTaskAndReturn(mappingJobExecution: FhirMappingJobExecution,
                                           sourceSettings: Map[String, DataSourceSettings],
                                           terminologyServiceSettings: Option[TerminologyServiceSettings] = None,
                                           identityServiceSettings: Option[IdentityServiceSettings] = None,
                                          ): Future[Seq[FhirMappingResult]] = {
    readSourceAndExecuteTask(mappingJobExecution.jobId, mappingJobExecution.mappingTasks.head, sourceSettings, terminologyServiceSettings, identityServiceSettings, executionId = Some(mappingJobExecution.id), projectId = Some(mappingJobExecution.projectId))
      .map { dataFrame =>
        dataFrame
          .collect() // Collect into an Array[String]
          .toSeq // Convert to Seq[Resource]
      }
  }

  override def executeMappingJobAndReturn(mappingJobExecution: FhirMappingJobExecution,
                                          sourceSettings: Map[String, DataSourceSettings],
                                          terminologyServiceSettings: Option[TerminologyServiceSettings],
                                          identityServiceSettings: Option[IdentityServiceSettings],
                                          taskCompletionCallback: () => Unit): Future[Dataset[FhirMappingResult]] = {
    import spark.implicits._
    // create an initial empty DataFrame
    val initialDataFrame: Dataset[FhirMappingResult] = spark.emptyDataset[FhirMappingResult]
    // fold over the mapping tasks to chain the futures sequentially
    mappingJobExecution.mappingTasks.foldLeft(Future.successful(initialDataFrame)) { (accFuture, task) =>
      accFuture.flatMap { accDataFrame =>
        logger.info(s"Executing mapping task ${task.mappingRef} within mapping job: ${mappingJobExecution.jobId}")
        readSourceAndExecuteTask(mappingJobExecution.jobId, task, sourceSettings, terminologyServiceSettings, identityServiceSettings, executionId = Some(mappingJobExecution.id), projectId = Some(mappingJobExecution.projectId))
          .map { dataFrame =>
            logger.info(s"Completed the execution of mapping task ${task.mappingRef} within mapping job: ${mappingJobExecution.jobId}")
            // notify the caller that the mapping task execution is complete by invoking the taskCompletionCallback function
            taskCompletionCallback()
            // combine the accumulated DataFrame with the current task's DataFrame
            accDataFrame.union(dataFrame)
          }.recover {
            case e: Throwable =>
              logger.error(s"Failed to execute mapping task ${task.mappingRef} within mapping job: ${mappingJobExecution.jobId}",e)
              throw e
          }
      }
    }
  }
}


