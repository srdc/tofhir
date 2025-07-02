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
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

class FileStreamingTest extends AnyFlatSpec with BeforeAndAfterAll with ToFhirTestSpec {

  import io.tofhir.engine.Execution.actorSystem.dispatcher

  /**
   * Constants for paths and settings
   */
  // The temporary directory location where test-specific folders will be created
  val tmpDirsLocation: String = FileUtils.getPath("streaming-test-folder").toAbsolutePath.toString
  // Path to the folder containing test data files (e.g., CSV, JSON, Parquet) used in tests.
  val testDataFolderPath: String = Paths.get(getClass.getResource("/test-data").toURI).toAbsolutePath.toString
  // Base path for folders used for streaming test input and output
  val streamingTestWatchFolderPath: String = FileUtils.getPath(tmpDirsLocation, "test-streaming/watch").toAbsolutePath.toString
  // Path to the output file where FHIR resources will be written in NDJSON format
  val fileSinkPath: String = FileUtils.getPath(tmpDirsLocation, "test-streaming/output/fhir-resources.ndjson").toAbsolutePath.toString

  // Map for watch folders
  // These folders are used as input directories for streaming data during tests
  val watchFolders: Map[String, File] = Map(
    "patientWatchFolder" -> FileUtils.getPath(streamingTestWatchFolderPath, "patients_csv").toFile,
    "jsonWatchFolder" -> FileUtils.getPath(streamingTestWatchFolderPath, "patients_json").toFile,
    "parquetWatchFolder" -> FileUtils.getPath(streamingTestWatchFolderPath, "patients_parquet").toFile
  )

  /**
   * FHIR mapping job to use in tests
   */
  // Sink and source settings of the job to be used
  val fileSinkSettings: FileSystemSinkSettings = FileSystemSinkSettings(path = fileSinkPath, contentType = SinkContentTypes.NDJSON)
  val mappingJobSourceSettings: Map[String, MappingJobSourceSettings] = Map(
    "test-source" -> FileSystemSourceSettings(name = "test-source", sourceUri = "https://aiccelerate.eu/data-integration-suite/test-data", dataFolderPath = streamingTestWatchFolderPath, asStream = true)
  )
  // Create a fhir mapping job for testing
  val fhirMappingJob: FhirMappingJob = FhirMappingJob(
    name = Some("test-streaming-job"),
    sourceSettings = mappingJobSourceSettings,
    sinkSettings = fileSinkSettings,
    mappings = Seq.empty,
    dataProcessingSettings = DataProcessingSettings()
  )

  val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, Map(FhirPathUtilFunctionsFactory.defaultPrefix -> FhirPathUtilFunctionsFactory), sparkSession)

  override def beforeAll(): Unit = {
    createStreamingTestFolders()
    prepareDataSources()
  }

  override def afterAll(): Unit = {
    org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(tmpDirsLocation).toFile)
  }

  /**
   * Creates all necessary directories for streaming tests before running.
   * It creates the directories specified in the `watchFolders` maps.
   */
  private def createStreamingTestFolders(): Unit = {
    watchFolders.foreach(
      folder => folder._2.mkdirs()
    )

  }

  /**
   * Prepares the data source for streaming tests by copying test data files to the appropriate watch folders.
   */
  private def prepareDataSources(): Unit = {
    // Copy CSV files to their test directories
    val patientWatchFolder: String = watchFolders("patientWatchFolder").getAbsolutePath
    org.apache.commons.io.FileUtils.copyFile(FileUtils.getPath(testDataFolderPath, "patients.csv").toFile, FileUtils.getPath(patientWatchFolder, "patients.csv").toFile)
    // Copy JSON file to test directory
    val jsonWatchPath: String = watchFolders("jsonWatchFolder").getAbsolutePath
    org.apache.commons.io.FileUtils.copyFile(FileUtils.getPath(testDataFolderPath, "patients.json").toFile, FileUtils.getPath(jsonWatchPath, "patients.json").toFile)
    // Copy Parquet file to test directory
    val parquetWatchFolder: String = watchFolders("parquetWatchFolder").getAbsolutePath
    org.apache.commons.io.FileUtils.copyFile(FileUtils.getPath(testDataFolderPath, "patients.parquet").toFile, FileUtils.getPath(parquetWatchFolder, "patients.parquet").toFile)
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
   * - Starting the mapping job stream and checking the status of the streaming queries.
   *
   */
  it should "start a streaming job reading from CSV files" in {
    // Patient Mapping Task
    val patientMappingTask: FhirMappingTask = FhirMappingTask(
      name = "patient-mapping",
      mappingRef = "https://aiccelerate.eu/fhir/mappings/patient-mapping",
      sourceBinding = Map("source" -> FileSystemSource(
        path = "patients_csv",
        contentType = SourceContentTypes.CSV,
        sourceRef = Some("test-source")
      ))
    )
    // clean up the checkpoint directory so that spark does not fail
    val mappingJobExecution = FhirMappingJobExecution(mappingTasks = Seq(patientMappingTask), job = fhirMappingJob)
    val checkpointDirectory: File = new File(mappingJobExecution.getCheckpointDirectory(patientMappingTask.name))
    org.apache.commons.io.FileUtils.deleteDirectory(checkpointDirectory)
    // Run the streaming job, wait for all streaming queries to complete and check their status
    val streamingQueryFutures = fhirMappingJobManager.startMappingJobStream(mappingJobExecution = mappingJobExecution, mappingJobSourceSettings, fileSinkSettings)
    // Wait for future to complete first
    val streamingQueries = Await.result(Future.sequence(streamingQueryFutures.values), 10.seconds)
    streamingQueries.foreach(sq => sq.isActive shouldBe true)
    // Wait for 5 seconds to allow the streaming queries to process data
    streamingQueries.foreach(sq => sq.awaitTermination(10000))
    streamingQueries.foreach(sq => sq.stop() shouldBe ())
  }

  /**
   * Test to verify starting a streaming job that reads from JSON files.
   *
   * This test ensures that a streaming job can be correctly started when reading from JSON files
   * It includes:
   * - Setting up a `FhirMappingTask` instance for JSON data.
   * - Starting the mapping job stream and checking the status of the streaming queries.
   *
   */
  it should "start a streaming job reading from JSON files" in {
    // Json Mapping Task
    val jsonMappingTask: FhirMappingTask = FhirMappingTask(
      name = "patient-mapping",
      mappingRef = "https://aiccelerate.eu/fhir/mappings/patient-mapping",
      sourceBinding = Map("source" -> FileSystemSource(
        path = "patients_json",
        contentType = SourceContentTypes.JSON,
        options = Map("multiLine" -> "true"),
        sourceRef = Some("test-source")
      ))
    )
    // clean up the checkpoint directory so that spark does not fail
    val mappingJobExecution = FhirMappingJobExecution(mappingTasks = Seq(jsonMappingTask), job = fhirMappingJob)
    val checkpointDirectory: File = new File(mappingJobExecution.getCheckpointDirectory(jsonMappingTask.name))
    org.apache.commons.io.FileUtils.deleteDirectory(checkpointDirectory)
    // Run the streaming job, wait for all streaming queries to complete and check their status
    val streamingQueryFutures = fhirMappingJobManager.startMappingJobStream(mappingJobExecution, mappingJobSourceSettings, fileSinkSettings)
    // Wait for future to complete first
    val streamingQueries = Await.result(Future.sequence(streamingQueryFutures.values), 10.seconds)
    streamingQueries.foreach(sq => sq.isActive shouldBe true)
    // Wait for 5 seconds to allow the streaming queries to process data
    streamingQueries.foreach(sq => sq.awaitTermination(10000))
    streamingQueries.foreach(sq => sq.stop() shouldBe ())
  }

  /**
   * Test to verify starting a streaming job that reads from Parquet files.
   *
   * This test ensures that a streaming job can be correctly started when reading from Parquet files
   * It includes:
   * - Setting up a `FhirMappingTask` instance for Parquet data.
   * - Starting the mapping job stream and checking the status of the streaming queries.
   *
   */
  it should "start a streaming job reading from Parquet files" in {
    // Parquet Mapping Task
    val parquetMappingTask: FhirMappingTask = FhirMappingTask(
      name = "patient-mapping",
      mappingRef = "https://aiccelerate.eu/fhir/mappings/patient-mapping",
      sourceBinding = Map("source" -> FileSystemSource(
        path = "patients_parquet",
        contentType = SourceContentTypes.PARQUET,
        sourceRef = Some("test-source")
      ))
    )
    // clean up the checkpoint directory so that spark does not fail
    val mappingJobExecution = FhirMappingJobExecution(mappingTasks = Seq(parquetMappingTask), job = fhirMappingJob)
    val checkpointDirectory: File = new File(mappingJobExecution.getCheckpointDirectory(parquetMappingTask.name))
    org.apache.commons.io.FileUtils.deleteDirectory(checkpointDirectory)
    // Run the streaming job, wait for all streaming queries to complete and check their status
    val streamingQueryFutures = fhirMappingJobManager.startMappingJobStream(mappingJobExecution, mappingJobSourceSettings, fileSinkSettings)
    // Wait for future to complete first
    val streamingQueries = Await.result(Future.sequence(streamingQueryFutures.values), 10.seconds)
    streamingQueries.foreach(sq => sq.isActive shouldBe true)
    // Wait for 5 seconds to allow the streaming queries to process data
    streamingQueries.foreach(sq => sq.awaitTermination(10000))
    streamingQueries.foreach(sq => sq.stop() shouldBe ())
  }


  /**
   * Test to verify if the streaming jobs mapped the data correctly.
   */
  it should "check if the streaming jobs mapped the data correctly" in {
    // Check if the data is written correctly to the output folder
    val outputFolder = new File(fileSinkPath)
    assert(outputFolder.exists(), "Output file does not exist")
    assert(outputFolder.length() > 0, "Output file is empty")
    // number of files in output data with extension .txt should be 3
    val expectedFileCount = 3
    val actualFileCount = outputFolder.listFiles().count(_.getName.endsWith(".txt"))
    assert(actualFileCount == expectedFileCount, s"Expected $expectedFileCount mapped files, but found $actualFileCount")
  }
}
