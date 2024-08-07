package io.tofhir.test

import io.onfhir.path.FhirPathUtilFunctionsFactory
import io.tofhir.ToFhirTestSpec
import io.tofhir.engine.data.write.FileSystemWriter.SinkFileFormats
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

  val testStreamingMappingJobFilePath: String = getClass.getResource("/streaming-job-example.json").toURI.getPath

  val tmpDirsLocation: String = System.getProperty("java.io.tmpdir")
  val streamingTestWatchFolderPath: String = FileUtils.getPath(tmpDirsLocation, "test-streaming/watch").toAbsolutePath.toString
  val streamingTestArchiveFolderPath: String = FileUtils.getPath(tmpDirsLocation, "test-streaming/archive").toAbsolutePath.toString

  val patientWatchFolder: File = FileUtils.getPath(streamingTestWatchFolderPath, "patients_csv").toFile
  val observationWatchFolder: File = FileUtils.getPath(streamingTestWatchFolderPath, "observations_csv").toFile
  val patientArchiveFolder: File = FileUtils.getPath(streamingTestArchiveFolderPath, "patients_csv").toFile
  val observationArchiveFolder: File = FileUtils.getPath(streamingTestArchiveFolderPath, "observations_csv").toFile

  val testDataFolderPath: String = Paths.get(getClass.getResource("/test-data").toURI).toAbsolutePath.toString

  val dataSourceSettings: Map[String, DataSourceSettings] = Map(
    "source" ->
      FileSystemSourceSettings(name = "test-source", sourceUri = "https://aiccelerate.eu/data-integration-suite/test-data",
        dataFolderPath = streamingTestWatchFolderPath, asStream = true))

  val fileSinkSettings: FileSystemSinkSettings = FileSystemSinkSettings(path = FileUtils.getPath(tmpDirsLocation, "test-streaming/output/fhir-resources.ndjson").toAbsolutePath.toString, fileFormat = Some(SinkFileFormats.NDJSON))

  val patientMappingTask: FhirMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/patient-mapping",
    sourceContext = Map("source" -> FileSystemSource(path = "patients_csv", fileFormat = Some("csv"), options = Map("cleanSource" -> "archive", "sourceArchiveDir" -> patientArchiveFolder.getAbsolutePath)))
  )

  val observationMappingTask: FhirMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/other-observation-mapping",
    sourceContext = Map("source" -> FileSystemSource(path = "observations_csv", fileFormat = Some("csv"), options = Map("cleanSource" -> "archive", "sourceArchiveDir" -> observationArchiveFolder.getAbsolutePath)))
  )

  //create a fhir mapping job for testing
  val fhirMappingJob: FhirMappingJob = FhirMappingJob(
    name = Some("test-streaming-job"),
    sourceSettings = dataSourceSettings,
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

  private def createStreamingTestFolders(): Unit = {
    patientWatchFolder.mkdirs()
    observationWatchFolder.mkdirs()
    patientArchiveFolder.mkdirs()
    observationArchiveFolder.mkdirs()
  }

  private def cleanUp(): Unit = {
    //org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(streamingTestWatchFolderPath).toFile)
  }

  it should "read a streaming mapping job definition" in {
    val streamingMappingJob = FhirMappingJobFormatter.readMappingJobFromFile(testStreamingMappingJobFilePath)
    streamingMappingJob.sourceSettings("source").asStream shouldBe true
    streamingMappingJob.sinkSettings shouldBe a[FileSystemSinkSettings]
  }

  it should "start a streaming job reading from a folder" in {

    org.apache.commons.io.FileUtils.copyFile(FileUtils.getPath(testDataFolderPath, "patients.csv").toFile, FileUtils.getPath(patientWatchFolder.getAbsolutePath, "patients.csv").toFile)
    org.apache.commons.io.FileUtils.copyFile(FileUtils.getPath(testDataFolderPath, "other-observations.csv").toFile, FileUtils.getPath(observationWatchFolder.getAbsolutePath, "other-observations.csv").toFile)

    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, Map(FhirPathUtilFunctionsFactory.defaultPrefix -> FhirPathUtilFunctionsFactory), sparkSession)
    val streamingQueryFutures = fhirMappingJobManager.startMappingJobStream(mappingJobExecution = FhirMappingJobExecution(mappingTasks = Seq(patientMappingTask, observationMappingTask), job = fhirMappingJob), dataSourceSettings, fileSinkSettings)
    val streamingQueries = Await.result(Future.sequence(streamingQueryFutures.values), FiniteDuration(5, TimeUnit.SECONDS))
    streamingQueries.foreach(sq => sq.isActive shouldBe true)
//    streamingQuery.awaitTermination()
    streamingQueries.foreach(sq => sq.stop() shouldBe ())
//    println("TESTINNGGGG") shouldBe ()
  }

}
