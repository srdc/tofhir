package io.onfhir.tofhir.engine

import com.typesafe.scalalogging.Logger
import io.onfhir.api.Resource
import io.onfhir.tofhir.config.MappingErrorHandling
import io.onfhir.tofhir.config.MappingErrorHandling.MappingErrorHandling
import io.onfhir.tofhir.data.read.DataSourceReaderFactory
import io.onfhir.tofhir.data.write.FhirWriterFactory
import io.onfhir.tofhir.model._
import io.onfhir.util.JsonFormatter._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.json4s.ext.URISerializer
import org.json4s.ext.EnumNameSerializer
import org.json4s.jackson.Serialization
import org.json4s.{Formats, JObject, ShortTypeHints}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeoutException
import scala.collection.IterableOnce.iterableOnceExtensionMethods
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
                             mappingErrorHandling: MappingErrorHandling
                           )(implicit ec: ExecutionContext) extends IFhirMappingJobManager {

  private val logger: Logger = Logger(this.getClass)

  /**
   * Execute the given mapping job and write the resulting FHIR resources to given sink
   *
   * @param id           Unique job identifier
   * @param tasks        Mapping tasks that will be executed in sequential
   * @param sinkSettings FHIR sink settings (can be a FHIR repository, file system, kafka)
   * @return
   */
  override def executeMappingJob(id: String, tasks: Seq[FhirMappingTask], sinkSettings: FhirSinkSettings): Future[Unit] = {
    val fhirWriter = FhirWriterFactory.apply(sinkSettings)
    tasks.foldLeft(Future((): Unit)) { (f, task) => // Initial empty Future
      f.flatMap { _ => // Execute the Futures in the Sequence consecutively (not in parallel)
        executeTask(task) // Retrieve the source data and execute the mapping
          .map(dataset => fhirWriter.write(dataset)) // Write the created FHIR Resources to the FhirWriter
      }
    } map { _ => logger.debug(s"MappingJob execution finished for MappingJob: $id.") }
  }

  /**
   * Execute the given mapping task and write the resulting FHIR resources to the given sink
   *
   * @param id           Unique job identifier
   * @param task         A Mapping task that will be executed
   * @param sinkSettings FHIR sink settings (can be a FHIR repository, file system, kafka)
   * @return
   */
  override def executeMappingTask(id: String, task: FhirMappingTask, sinkSettings: FhirSinkSettings): Future[Unit] = {
    val fhirWriter = FhirWriterFactory.apply(sinkSettings)
    executeTask(task) map { f => fhirWriter.write(f) }
  }

  /**
   * Execute a single mapping task.
   *
   * @param task A #FhirMappingTask to be executed.
   * @return
   */
  private def executeTask(task: FhirMappingTask): Future[Dataset[String]] = {
    //Retrieve the FHIR mapping definition
    val fhirMapping = fhirMappingRepository.getFhirMappingByUrl(task.mappingRef)
    val sourceNames = fhirMapping.source.map(_.alias).toSet
    val namesForSuppliedSourceContexts = task.sourceContext.keySet
    if (sourceNames != namesForSuppliedSourceContexts)
      throw FhirMappingException(s"Invalid mapping task, source context is not given for some mapping source(s) ${sourceNames.diff(namesForSuppliedSourceContexts).mkString(", ")}")

    //Get the source schemas
    val sources = fhirMapping.source.map(s => (s.alias, schemaLoader.getSchema(s.url), task.sourceContext(s.alias)))
    //Read sources into Spark as DataFrame
    val sourceDataFrames =
      sources.map {
        case (alias, schema, sourceContext) =>
          alias ->
            DataSourceReaderFactory
              .apply(spark, sourceContext)
              .read(sourceContext, schema)
      }

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
      val configurationContext = task.sourceContext(sourceNames.head).settings.toConfigurationContext
      //Construct the mapping service
      val fhirMappingService = new FhirMappingService(fhirMapping.source.map(_.alias), (loadedContextMap :+ configurationContext).toMap, fhirMapping.mapping)
      MappingTaskExecutor.executeMapping(spark, df, fhirMappingService, mappingErrorHandling)
    })
  }

  /**
   * Handle the joining of source data framees
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
   * @param id             Unique job identifier
   * @param sourceSettings Data source settings and configurations
   * @param task           Mapping task that will be executed
   * @return
   */
  override def executeMappingTaskAndReturn(id: String, task: FhirMappingTask): Future[Seq[JObject]] = {
    executeTask(task)
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
   * @param row
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
   * @param spark
   * @param df
   * @param fhirMappingService
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
   * @param spark
   * @param df
   * @param fhirMappingService
   * @return
   */
  private def executeMappingOnSingleSource(spark: SparkSession, df: DataFrame, fhirMappingService: FhirMappingService, errorHandlingType: MappingErrorHandling): Dataset[String] = {
    import spark.implicits._
    val result =
      df
        .flatMap(row => {
          val jo = convertRowToJObject(row)
          val resources = try {
            Await.result(fhirMappingService.mapToFhir(jo), Duration.apply(5, "seconds"))
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

object FhirMappingJobManager {

  implicit lazy val formats: Formats =
    Serialization.formats(ShortTypeHints(List(classOf[FhirRepositorySinkSettings], classOf[FileSystemSource], classOf[FileSystemSourceSettings]))) +
      new EnumNameSerializer(MappingErrorHandling)

  def saveMappingJobToFile(fhirMappingJob: FhirMappingJob, filePath: String): Unit = {
    Files.write(Paths.get(filePath), Serialization.writePretty(fhirMappingJob).getBytes(StandardCharsets.UTF_8))
  }

  def readMappingJobFromFile(filePath: String): FhirMappingJob = {
    val source = Source.fromFile(filePath, StandardCharsets.UTF_8.name())
    val fileContent = try source.mkString finally source.close()
    org.json4s.jackson.JsonMethods.parse(fileContent).extract[FhirMappingJob]
  }
}
