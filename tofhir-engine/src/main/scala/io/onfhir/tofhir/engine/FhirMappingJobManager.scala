package io.onfhir.tofhir.engine

import com.typesafe.scalalogging.Logger
import io.onfhir.api.Resource
import io.onfhir.tofhir.config.{MappingErrorHandling, ToFhirConfig}
import io.onfhir.tofhir.config.MappingErrorHandling.MappingErrorHandling
import io.onfhir.tofhir.data.read.DataSourceReaderFactory
import io.onfhir.tofhir.data.write.FhirWriterFactory
import io.onfhir.tofhir.model._
import io.onfhir.util.JsonFormatter._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.json4s.ext.EnumNameSerializer
import org.json4s.jackson.Serialization
import org.json4s.{Formats, JObject, ShortTypeHints}

import java.io.{File, FileNotFoundException, FileWriter}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.{Instant, LocalDateTime, ZoneOffset}
import java.util.concurrent.TimeoutException
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
 * @param ec
 */
class FhirMappingJobManager(
                             fhirMappingRepository: IFhirMappingRepository,
                             contextLoader: IMappingContextLoader,
                             schemaLoader: IFhirSchemaLoader,
                             spark: SparkSession,
                             mappingErrorHandling: MappingErrorHandling,
                             mappingJobScheduler: Option[MappingJobScheduler] = Option.empty
                           )(implicit ec: ExecutionContext) extends IFhirMappingJobManager {

  private val logger: Logger = Logger(this.getClass)

  /**
   * Execute the given mapping job and write the resulting FHIR resources to given sink
   *
   * @param id              Unique job identifier
   * @param tasks           Mapping tasks that will be executed in sequential
   * @param sourceSettings  Settings for the source system(s)
   * @param sinkSettings    FHIR sink settings (can be a FHIR repository, file system, kafka)
   * @param terminologyServiceSettings  Settings for terminology service to use within mappings (e.g. lookupDisplay)
   * @param identityServiceSettings     Settings for identity service to use within mappings (e.g. resolveIdentifier)
   * @param timeRange       Time range for source data to map if given
   * @return
   */
  override def executeMappingJob(id: String,
                                 tasks: Seq[FhirMappingTask],
                                 sourceSettings: Map[String,DataSourceSettings],
                                 sinkSettings: FhirSinkSettings,
                                 terminologyServiceSettings: Option[TerminologyServiceSettings] = None,
                                 identityServiceSettings:Option[IdentityServiceSettings] = None,
                                 timeRange: Option[(LocalDateTime, LocalDateTime)] = None): Future[Unit] = {
    val fhirWriter = FhirWriterFactory.apply(sinkSettings)

    tasks.foldLeft(Future((): Unit)) { (f, task) => // Initial empty Future
      f.flatMap { _ => // Execute the Futures in the Sequence consecutively (not in parallel)
        executeTask(task, sourceSettings,terminologyServiceSettings, identityServiceSettings, timeRange) // Retrieve the source data and execute the mapping
          .map(dataset => fhirWriter.write(dataset)) // Write the created FHIR Resources to the FhirWriter
      }
    } map { _ => logger.debug(s"MappingJob execution finished for MappingJob: $id.") }
  }

  /**
   * Start streaming mapping job
   *
   * @param id           Job identifier
   * @param tasks        Mapping tasks that will be executed in parallel in stream mode
   * @param sourceSettings  Settings for the source system(s)
   * @param sinkSettings FHIR sink settings (can be a FHIR repository, file system, kafka)
   * @param terminologyServiceSettings  Settings for terminology service to use within mappings (e.g. lookupDisplay)
   * @param identityServiceSettings     Settings for identity service to use within mappings (e.g. resolveIdentifier)
   * @return
   */
  override def startMappingJobStream(id: String,
                                     tasks: Seq[FhirMappingTask],
                                     sourceSettings: Map[String,DataSourceSettings],
                                     sinkSettings: FhirSinkSettings,
                                     terminologyServiceSettings: Option[TerminologyServiceSettings] = None,
                                     identityServiceSettings:Option[IdentityServiceSettings] = None,
                                    ): StreamingQuery = {
    val fhirWriter = FhirWriterFactory.apply(sinkSettings)

    val mappedResourcesDf =
      tasks
        .map(t => Await.result(executeTask(t, sourceSettings, terminologyServiceSettings, identityServiceSettings), Duration.Inf))
        .reduce((ts1, ts2) => ts1.union(ts2))

    val datasetWrite = (dataset: Dataset[String], batchN: Long) => fhirWriter.write(dataset)

    mappedResourcesDf
      .writeStream
      .foreachBatch(datasetWrite)
      .start()
  }

  /**
   * Schedule to execute the given mapping job with given cron expression and write the resulting FHIR resources to the given sink
   *
   * @param id                Unique job identifier
   * @param tasks             Mapping tasks that will be executed in sequential
   * @param sourceSettings    Settings for the source system(s)
   * @param sinkSettings      FHIR sink settings (can be a FHIR repository, file system, kafka)
   * @param schedulingSettings  Settings for scheduling the job
   * @param terminologyServiceSettings  Settings for terminology service to use within mappings (e.g. lookupDisplay)
   * @param identityServiceSettings     Settings for identity service to use within mappings (e.g. resolveIdentifier)
   * @return
   */
  override def scheduleMappingJob(id: String,
                                  tasks: Seq[FhirMappingTask],
                                  sourceSettings: Map[String,DataSourceSettings],
                                  sinkSettings: FhirSinkSettings,
                                  schedulingSettings: SchedulingSettings,
                                  terminologyServiceSettings: Option[TerminologyServiceSettings] = None,
                                  identityServiceSettings:Option[IdentityServiceSettings] = None,
                                 ): Unit = {
    val startTime = if (schedulingSettings.initialTime.isEmpty) {
      logger.info(s"initialTime is not specified in the mappingJob. I will sync all the data from midnight, January 1, 1970 to the next run time.")
      Instant.ofEpochMilli(0L).atOffset(ZoneOffset.UTC).toLocalDateTime
    } else {
      LocalDateTime.parse(schedulingSettings.initialTime.get)
    }
    // Schedule a task
    val taskId = mappingJobScheduler.get.scheduler.schedule(schedulingSettings.cronExpression, new Runnable() {
      override def run(): Unit = {
        val scheduledJob = runnableMappingJob(id, startTime, tasks, sourceSettings, sinkSettings, terminologyServiceSettings, identityServiceSettings, schedulingSettings)
        Await.result(scheduledJob, Duration.Inf)
      }
    })
  }

  /**
   * Runnable for scheduled periodic mapping job
   *
   * @param id                 Job identifier
   * @param startTime          Initial start time for source data
   * @param tasks              Mapping tasks
   * @param sourceSettings      Settings for the source system
   * @param sinkSettings       FHIR sink settings/configurations
   * @param schedulingSettings Scheduling information
   * @return
   */
  private def runnableMappingJob(id: String,
                                 startTime: LocalDateTime,
                                 tasks: Seq[FhirMappingTask],
                                 sourceSettings: Map[String,DataSourceSettings],
                                 sinkSettings: FhirSinkSettings,
                                 terminologyServiceSettings: Option[TerminologyServiceSettings] = None,
                                 identityServiceSettings:Option[IdentityServiceSettings] = None,
                                 schedulingSettings: SchedulingSettings): Future[Unit] = {
    val timeRange = getScheduledTimeRange(id, mappingJobScheduler.get.folderUri, startTime)
    logger.info(s"Running scheduled job with the expression: ${schedulingSettings.cronExpression}")
    logger.info(s"Synchronizing data between ${timeRange._1} and ${timeRange._2}")
    executeMappingJob(id, tasks, sourceSettings, sinkSettings, terminologyServiceSettings, identityServiceSettings, Some(timeRange))
      .map(_ => {
        val writer = new FileWriter(s"${mappingJobScheduler.get.folderUri.getPath}/$id.txt", true)
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
    if (!new File(folderUri).exists || !new File(folderUri).isDirectory) throw new FileNotFoundException(s"Folder cannot be found: ${folderUri.toString}")
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
   * @param id                Unique job identifier
   * @param task              A Mapping task that will be executed
   * @param sourceSettings    Settings for the source system(s)
   * @param sinkSettings      FHIR sink settings (can be a FHIR repository, file system, kafka)
   * @param terminologyServiceSettings  Settings for terminology service to use within mappings (e.g. lookupDisplay)
   * @param identityServiceSettings     Settings for identity service to use within mappings (e.g. resolveIdentifier)
   * @return
   */
  override def executeMappingTask(id: String,
                                  task: FhirMappingTask,
                                  sourceSettings: Map[String,DataSourceSettings],
                                  sinkSettings: FhirSinkSettings,
                                  terminologyServiceSettings: Option[TerminologyServiceSettings] = None,
                                  identityServiceSettings:Option[IdentityServiceSettings] = None,
                                 ): Future[Unit] = {
    val fhirWriter = FhirWriterFactory.apply(sinkSettings)

    executeTask(task, sourceSettings, terminologyServiceSettings, identityServiceSettings) map { dataset => fhirWriter.write(dataset) }
  }

  /**
   * Execute a single mapping task.
   *
   * @param task              A #FhirMappingTask to be executed.
   * @param sourceSettings    Settings for the source system
   * @param timeRange         Time range for the source data to load
   * @return
   */
  private def executeTask(task: FhirMappingTask,
                          sourceSettings: Map[String,DataSourceSettings],
                          terminologyServiceSettings: Option[TerminologyServiceSettings] = None,
                          identityServiceSettings:Option[IdentityServiceSettings] = None,
                          timeRange: Option[(LocalDateTime, LocalDateTime)] = None): Future[Dataset[String]] = {
    //Retrieve the FHIR mapping definition
    val fhirMapping = fhirMappingRepository.getFhirMappingByUrl(task.mappingRef)
    val sourceNames = fhirMapping.source.map(_.alias).toSet
    val namesForSuppliedSourceContexts = task.sourceContext.keySet
    if (sourceNames != namesForSuppliedSourceContexts)
      throw FhirMappingException(s"Invalid mapping task, source context is not given for some mapping source(s) ${sourceNames.diff(namesForSuppliedSourceContexts).mkString(", ")}")

    //Get the source schemas
    val sources = fhirMapping.source.map(s => (s.alias, schemaLoader.getSchema(s.url), task.sourceContext(s.alias), sourceSettings.apply(s.alias),  timeRange))
    //Read sources into Spark as DataFrame
    val sourceDataFrames =
      sources.map {
        case (alias, schema, sourceContext, sourceStt, timeRange) =>
          alias ->
            DataSourceReaderFactory
              .apply(spark, sourceContext, sourceStt)
              .read(sourceContext, sourceStt, schema, timeRange)
      }
    //TODO Implement multiple source mappings, and input data resolution at those cases
    val df = handleJoin(task, sourceDataFrames)

    //df.printSchema()
    //df.show(10)

    //Load the contextual data for the mapping
    Future
      .sequence(
        fhirMapping
          .context
          .toSeq
          .map(cdef => contextLoader.retrieveContext(cdef._2).map(context => cdef._1 -> context))
      ).map(loadedContextMap => {
        //Get configuration context
        val sourceNames = fhirMapping.source.map(_.alias)
        val configurationContext = sources.head._4.toConfigurationContext
        //Construct the mapping service
        val fhirMappingService = new FhirMappingService(fhirMapping.source.map(_.alias), (loadedContextMap :+ configurationContext).toMap, fhirMapping.mapping, terminologyServiceSettings, identityServiceSettings)
        MappingTaskExecutor.executeMapping(spark, df, fhirMappingService, mappingErrorHandling)
    })
  }

  /**
   * Handle the joining of source data frames
   *
   * @param task             Mapping task definition
   * @param sourceDataFrames Source data frames loaded
   * @return
   */
  private def handleJoin(task: FhirMappingTask, sourceDataFrames: Seq[(String, DataFrame)]): DataFrame = {
    sourceDataFrames match {
      case Seq(_ -> df) => df
      case oth =>
        //Join of source data is not implemented yet
        throw new NotImplementedError()
    }
  }

  /**
   * Execute the given mapping job and return the resulting FHIR resources
   *
   * @param id                Unique job identifier
   * @param task              Mapping task that will be executed
   * @param sourceSettings    Settings for the source system
   * @param terminologyServiceSettings  Settings for terminology service to use within mappings (e.g. lookupDisplay)
   * @param identityServiceSettings     Settings for identity service to use within mappings (e.g. resolveIdentifier)
   * @return
   */
  override def executeMappingTaskAndReturn(id: String,
                                           task: FhirMappingTask,
                                           sourceSettings: Map[String,DataSourceSettings],
                                           terminologyServiceSettings: Option[TerminologyServiceSettings] = None,
                                           identityServiceSettings:Option[IdentityServiceSettings] = None,
                                          ): Future[Seq[JObject]] = {
    executeTask(task, sourceSettings, terminologyServiceSettings, identityServiceSettings)
      .map { dataFrame =>
        dataFrame
          .collect() // Collect into an Array[String]
          .map(_.parseJson) // Parse each JSON String into FHIR Resource where Resource is a JObject
          .toSeq // Convert to Seq[Resource]
      }
  }
}

object MappingTaskExecutor {

  private val logger: Logger = Logger(this.getClass)

  /**
   * Convert input row for mapping to JObject
   *
   * @param row Row to be converted to JObject
   * @return
   */
  def convertRowToJObject(row: Row): JObject = {
    // Row class of Spark has a private method (jsonValue) which transforms a Row into a json4s JObject.
    // We hacked the class to make it accessible and used it to covert a Row into JObject.
    val method = row.getClass.getSuperclass.getInterfaces.apply(0).getMethod("jsonValue")
    method.setAccessible(true)
    method.invoke(row).asInstanceOf[JObject]
  }

  /**
   * Executing the mapping and returning the dataframe for FHIR resources
   *
   * @param spark              Spark session
   * @param df                 DataFrame to be mapped
   * @param fhirMappingService Mapping service for a specific FhirMapping together with contextual data and mapping scripts
   * @return
   */
  def executeMapping(spark: SparkSession, df: DataFrame, fhirMappingService: FhirMappingService, errorHandlingType: MappingErrorHandling): Dataset[String] = {
    fhirMappingService.sources match {
      case Seq(_) => executeMappingOnSingleSource(spark, df, fhirMappingService, errorHandlingType)
      //Executing on multiple sources
      case _ => throw new NotImplementedError()
    }
  }

  /**
   *
   * @param spark              Spark session
   * @param df                 DataFrame to be mapped
   * @param fhirMappingService Mapping service for a specific FhirMapping together with contextual data and mapping scripts
   * @return
   */
  private def executeMappingOnSingleSource(spark: SparkSession,
                                           df: DataFrame,
                                           fhirMappingService: FhirMappingService,
                                           errorHandlingType: MappingErrorHandling): Dataset[String] = {
    import spark.implicits._
    val result =
      df
        .flatMap(row => {
          val jo = convertRowToJObject(row)
          val resources = try {
            Await.result(fhirMappingService.mapToFhir(jo), ToFhirConfig.mappingTimeout)
          } catch {
            case e: FhirMappingException =>
              logger.error(e.getMessage, e)
              if (errorHandlingType == MappingErrorHandling.CONTINUE) {
                Seq.empty[Resource]
              } else {
                throw e
              }
            case e: TimeoutException =>
              logger.error(s"TimeoutException. A single row could not be mapped to FHIR in 5 seconds. The row JObject: ${Serialization.write(jo)}")
              if (errorHandlingType == MappingErrorHandling.CONTINUE) {
                logger.debug("Continuing the processing of mappings...")
                Seq.empty[Resource]
              } else {
                logger.debug("Will halt the mapping execution!")
                throw e
              }
          }
          resources.map(_.toJson)
        })
    result
  }

}

/**
 *
 */
object FhirMappingJobManager {

  implicit lazy val formats: Formats =
    Serialization
      .formats(
        ShortTypeHints(List(
          //Sink settings
          classOf[FhirRepositorySinkSettings],
          classOf[FileSystemSinkSettings],
          //Source types
          classOf[FileSystemSource],
          classOf[KafkaSource],
          classOf[SqlSource],
          //Source settings
          classOf[FileSystemSourceSettings],
          classOf[SqlSourceSettings],
          classOf[KafkaSourceSettings],
          // Authorization types
          classOf[BearerTokenAuthorizationSettings],
          classOf[BasicAuthenticationSettings],
          //Terminology setvices
          classOf[LocalFhirTerminologyServiceSettings]
     ))) +
      new EnumNameSerializer(MappingErrorHandling)


  /**
   *
   * @param fhirMappingJob
   * @param filePath
   */
  def saveMappingJobToFile(fhirMappingJob: FhirMappingJob, filePath: String): Unit = {
    Files.write(Paths.get(filePath), Serialization.writePretty(fhirMappingJob).getBytes(StandardCharsets.UTF_8))
  }

  /**
   *
   * @param filePath
   * @return
   */
  def readMappingJobFromFile(filePath: String): FhirMappingJob = {
    val source = Source.fromFile(filePath, StandardCharsets.UTF_8.name())
    val fileContent = try source.mkString finally source.close()
    org.json4s.jackson.JsonMethods.parse(fileContent).extract[FhirMappingJob]
  }
}
