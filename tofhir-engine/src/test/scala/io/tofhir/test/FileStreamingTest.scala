package io.tofhir.test

import io.onfhir.path.FhirPathUtilFunctionsFactory
import io.tofhir.ToFhirTestSpec
import io.tofhir.engine.data.write.FileSystemWriter.SinkContentTypes
import io.tofhir.engine.mapping.job.FhirMappingJobManager
import io.tofhir.engine.model._
import io.tofhir.engine.util.{FhirMappingJobFormatter, FileUtils}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.io.File
import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, Future}

class FileStreamingTest extends AnyFlatSpec with BeforeAndAfterAll with ToFhirTestSpec {

  import io.tofhir.engine.Execution.actorSystem.dispatcher

  /**
   * Constants for paths and settings
   */
  // The temporary directory location where test-specific folders will be created
  val tmpDirsLocation: String = System.getProperty("java.io.tmpdir")
  // Path to the folder containing test data files (e.g., CSV, JSON, Parquet) used in tests.
  val testDataFolderPath: String = Paths.get(getClass.getResource("/test-data").toURI).toAbsolutePath.toString
  // Base path for folders used for streaming test input and output
  val streamingTestWatchFolderPath: String = FileUtils.getPath(tmpDirsLocation, "test-streaming/watch").toAbsolutePath.toString
  val streamingTestArchiveFolderPath: String = FileUtils.getPath(tmpDirsLocation, "test-streaming/archive").toAbsolutePath.toString
  // Path to the output file where FHIR resources will be written in NDJSON format
  val fileSinkPath: String = FileUtils.getPath(tmpDirsLocation, "test-streaming/output/fhir-resources.ndjson").toAbsolutePath.toString

  // Map for watch folders
  // These folders are used as input directories for streaming data during tests
  val watchFolders: Map[String, File] = Map(
    "patientWatchFolder" -> FileUtils.getPath(streamingTestWatchFolderPath, "patients_csv").toFile,
    "observationWatchFolder" -> FileUtils.getPath(streamingTestWatchFolderPath, "observations_csv").toFile,
    "jsonWatchFolder" -> FileUtils.getPath(streamingTestWatchFolderPath, "patients_json").toFile,
    "parquetWatchFolder" -> FileUtils.getPath(streamingTestWatchFolderPath, "patients_parquet").toFile
  )

  // Map for archive folders
  // These folders are used to store archived files after processing during tests
  val archiveFolders: Map[String, File] = Map(
    "patientArchiveFolder" -> FileUtils.getPath(streamingTestArchiveFolderPath, "patients_csv").toFile,
    "observationArchiveFolder" -> FileUtils.getPath(streamingTestArchiveFolderPath, "observations_csv").toFile,
    "jsonArchiveFolder" -> FileUtils.getPath(streamingTestArchiveFolderPath, "patients_json").toFile,
    "parquetArchiveFolder" -> FileUtils.getPath(streamingTestArchiveFolderPath, "patients_parquet").toFile
  )

  /**
   * FHIR mapping job to use in tests
   */
  // Sink and source settings of the job to be used
  val fileSinkSettings: FileSystemSinkSettings = FileSystemSinkSettings(path = fileSinkPath, contentType = SinkContentTypes.NDJSON)
  val mappingJobSourceSettings: Map[String, MappingJobSourceSettings] = Map(
    "source" -> FileSystemSourceSettings(name = "test-source", sourceUri = "https://aiccelerate.eu/data-integration-suite/test-data", dataFolderPath = streamingTestWatchFolderPath, asStream = true)
  )
  // Create a fhir mapping job for testing
  val fhirMappingJob: FhirMappingJob = FhirMappingJob(
    name = Some("test-streaming-job"),
    sourceSettings = mappingJobSourceSettings,
    sinkSettings = fileSinkSettings,
    mappings = Seq.empty,
    dataProcessingSettings = DataProcessingSettings()
  )

  override def beforeAll(): Unit = {
    createStreamingTestFolders()
  }

  override def afterAll(): Unit = {
    cleanUp()
  }

  /**
   * Cleans up the watch folder used for testing.
   */
  private def cleanUp(): Unit = {
    //org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(streamingTestWatchFolderPath).toFile)
  }

  /**
   * Creates all necessary directories for streaming tests before running.
   * It creates the directories specified in the `watchFolders` and `archiveFolders` maps.
   */
  private def createStreamingTestFolders(): Unit = {
    watchFolders.foreach(
      folder => folder._2.mkdirs()
    )
    archiveFolders.foreach(
      folder => folder._2.mkdirs()
    )
  }

  /**
   * Test to verify reading a streaming mapping job definition from a JSON file.
   */
  it should "read a streaming mapping job definition" in {
    // Read a mapping job from a json file
    val streamingMappingJob = FhirMappingJobFormatter.readMappingJobFromFile(getClass.getResource("/streaming-job-example.json").toURI.getPath)

    // Validate asStream and sinkSettings properties are read correctly
    streamingMappingJob.sourceSettings("source").asStream shouldBe true
    streamingMappingJob.sinkSettings shouldBe a[FileSystemSinkSettings]
  }

  /**
   * Test to verify starting a streaming job that reads from folders.
   *
   * This test verifies that a streaming job can be correctly started when reading from CSV files
   * It includes:
   * - Setting up `FhirMappingTask` instances for CSV data.
   * - Copying test CSV data files to the designated watch and archive folders.
   * - Starting the mapping job stream and checking the status of the streaming queries.
   *
   */
  it should "start a streaming job reading from CSV files" in {
    // Patient Mapping Task
    val patientArchivePath: String = archiveFolders("patientArchiveFolder").getAbsolutePath
    val patientMappingTask: FhirMappingTask = FhirMappingTask(
      name = "patient-mapping",
      mappingRef = "https://aiccelerate.eu/fhir/mappings/patient-mapping",
      sourceBinding = Map("source" -> FileSystemSource(
        path = "patients_csv",
        contentType = SourceContentTypes.CSV,
        options = Map(
          "cleanSource" -> "archive",
          "sourceArchiveDir" -> patientArchivePath
        )
      ))
    )
    // Observation Mapping Task
    val ObservationArchivePath: String = archiveFolders("observationArchiveFolder").getAbsolutePath
    val observationMappingTask: FhirMappingTask = FhirMappingTask(
      name = "other-observation-mapping",
      mappingRef = "https://aiccelerate.eu/fhir/mappings/other-observation-mapping",
      sourceBinding = Map("source" -> FileSystemSource(
        path = "observations_csv",
        contentType = SourceContentTypes.CSV,
        options = Map(
          "cleanSource" -> "archive",
          "sourceArchiveDir" -> ObservationArchivePath
        )
      ))
    )
    // Copy files to their test directories
    org.apache.commons.io.FileUtils.copyFile(FileUtils.getPath(testDataFolderPath, "patients.csv").toFile, FileUtils.getPath(patientArchivePath, "patients.csv").toFile)
    org.apache.commons.io.FileUtils.copyFile(FileUtils.getPath(testDataFolderPath, "other-observations.csv").toFile, FileUtils.getPath(ObservationArchivePath, "other-observations.csv").toFile)

    // Run the streaming job, wait for all streaming queries to complete and check their status
    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, Map(FhirPathUtilFunctionsFactory.defaultPrefix -> FhirPathUtilFunctionsFactory), sparkSession)
    val streamingQueryFutures = fhirMappingJobManager.startMappingJobStream(mappingJobExecution = FhirMappingJobExecution(mappingTasks = Seq(patientMappingTask, observationMappingTask), job = fhirMappingJob), mappingJobSourceSettings, fileSinkSettings)
    val streamingQueries = Await.result(Future.sequence(streamingQueryFutures.values), FiniteDuration(5, TimeUnit.SECONDS))
    streamingQueries.foreach(sq => sq.isActive shouldBe true)
    streamingQueries.foreach(sq => sq.stop() shouldBe ())
  }

  /**
   * Test to verify starting a streaming job that reads from JSON files.
   *
   * This test ensures that a streaming job can be correctly started when reading from JSON files
   * It includes:
   * - Setting up a `FhirMappingTask` instance for JSON data.
   * - Copying a test JSON data file to the designated archive folder.
   * - Starting the mapping job stream and checking the status of the streaming queries.
   *
   */
  it should "start a streaming job reading from JSON files" in {
    // Json Mapping Task
    val jsonArchivePath: String = archiveFolders("observationArchiveFolder").getAbsolutePath
    val jsonMappingTask: FhirMappingTask = FhirMappingTask(
      name = "patient-mapping",
      mappingRef = "https://aiccelerate.eu/fhir/mappings/patient-mapping",
      sourceBinding = Map("source" -> FileSystemSource(
        path = "",
        contentType = SourceContentTypes.JSON,
        options = Map(
          "cleanSource" -> "archive",
          "sourceArchiveDir" -> jsonArchivePath
        )
      ))
    )
    // Copy json file to test directory
    org.apache.commons.io.FileUtils.copyFile(FileUtils.getPath(testDataFolderPath, "patients.json").toFile, FileUtils.getPath(jsonArchivePath, "patients.json").toFile)

    // Run the streaming job, wait for all streaming queries to complete and check their status
    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, Map(FhirPathUtilFunctionsFactory.defaultPrefix -> FhirPathUtilFunctionsFactory), sparkSession)
    val streamingQueryFutures = fhirMappingJobManager.startMappingJobStream(FhirMappingJobExecution(mappingTasks = Seq(jsonMappingTask), job = fhirMappingJob), mappingJobSourceSettings, fileSinkSettings)
    val streamingQueries = Await.result(Future.sequence(streamingQueryFutures.values), FiniteDuration(5, TimeUnit.SECONDS))
    streamingQueries.foreach(sq => sq.isActive shouldBe true)
    streamingQueries.foreach(sq => sq.stop() shouldBe ())
  }

  /**
   * Test to verify starting a streaming job that reads from Parquet files.
   *
   * This test ensures that a streaming job can be correctly started when reading from Parquet files
   * It includes:
   * - Setting up a `FhirMappingTask` instance for Parquet data.
   * - Copying a test Parquet data file to the designated archive folder.
   * - Starting the mapping job stream and checking the status of the streaming queries.
   *
   */
  it should "start a streaming job reading from Parquet files" in {
    // Parquet Mapping Task
    val parquetArchivePath: String = archiveFolders("observationArchiveFolder").getAbsolutePath
    val parquetMappingTask: FhirMappingTask = FhirMappingTask(
      name = "patient-mapping",
      mappingRef = "https://aiccelerate.eu/fhir/mappings/patient-mapping",
      sourceBinding = Map("source" -> FileSystemSource(
        path = "",
        contentType = SourceContentTypes.PARQUET,
        options = Map(
          "cleanSource" -> "archive",
          "sourceArchiveDir" -> parquetArchivePath
        )
      ))
    )
    // Copy parquet file to test directory
    org.apache.commons.io.FileUtils.copyFile(FileUtils.getPath(testDataFolderPath, "patients.parquet").toFile, FileUtils.getPath(parquetArchivePath, "patients.parquet").toFile)

    // Run the streaming job, wait for all streaming queries to complete and check their status
    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, Map(FhirPathUtilFunctionsFactory.defaultPrefix -> FhirPathUtilFunctionsFactory), sparkSession)
    val streamingQueryFutures = fhirMappingJobManager.startMappingJobStream(FhirMappingJobExecution(mappingTasks = Seq(parquetMappingTask), job = fhirMappingJob), mappingJobSourceSettings, fileSinkSettings)
    val streamingQueries = Await.result(Future.sequence(streamingQueryFutures.values), FiniteDuration(5, TimeUnit.SECONDS))
    streamingQueries.foreach(sq => sq.isActive shouldBe true)
    streamingQueries.foreach(sq => sq.stop() shouldBe ())
  }
}
