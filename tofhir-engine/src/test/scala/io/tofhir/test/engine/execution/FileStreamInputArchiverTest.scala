package io.tofhir.test.engine.execution

import io.tofhir.engine.config.ToFhirConfig
import io.tofhir.engine.execution.{FileStreamInputArchiver, RunningJobRegistry}
import io.tofhir.engine.model.{ArchiveModes, DataProcessingSettings, FhirMappingJob, FhirMappingJobExecution}
import io.tofhir.engine.util.FileUtils
import org.mockito.MockitoSugar._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.PrintWriter

class FileStreamInputArchiverTest extends AnyFlatSpec with Matchers {

  val runningJobRegistryMock = mock[RunningJobRegistry]
  val fileStreamInputArchiver = new FileStreamInputArchiver(runningJobRegistryMock)

  "FileStreamInputArchiver" should "apply archiving for a streaming job" in {

    // Create a mock execution
    val mockTaskExecution = mock[FhirMappingJobExecution]
    val mockJob = mock[FhirMappingJob]
    val mockDataProcessingSettings = mock[DataProcessingSettings]
    val mappingUrl = "mocked_mapping_url"
    val jobId = "mocked_job_id"
    when(mockTaskExecution.id).thenReturn(jobId)
    when(mockTaskExecution.job).thenReturn(mockJob)
    when(mockJob.dataProcessingSettings).thenReturn(mockDataProcessingSettings)
    when(mockDataProcessingSettings.archiveMode).thenReturn(ArchiveModes.ARCHIVE)
    when(mockTaskExecution.getCheckpointDirectory(mappingUrl)).thenReturn(
      FileUtils.getPath("test-archiver", jobId, mappingUrl.hashCode.toString).toString)


    // Create a source file to refer location of test.csv
    val sourceFile = FileUtils.getPath("test-archiver", jobId, mappingUrl.hashCode.toString, "sources", "0", "0").toFile
    // Path of test.csv
    val testCsvFile = FileUtils.getPath("test-archiver", "test.csv").toFile
    // Ensure the parent directories exist, if not, create them
    sourceFile.getParentFile.mkdirs()
    testCsvFile.getParentFile.mkdirs()
    // Write path of test.csv to the source file
    val sourceWriter = new PrintWriter(sourceFile)
    sourceWriter.write(s"{\"path\":\"${testCsvFile.getAbsolutePath.replace("\\", "\\\\")}\"}")
    sourceWriter.close()

    // Create a test csv
    val testCsvWriter = new PrintWriter(testCsvFile)
    testCsvWriter.write("testColumn1,testColumn2,testColumn3,testColumn4,testColumn5\ntestRow1,testRow2,testRow3,testRow4,testRow5")
    testCsvWriter.close()

    // Create a commit file inorder to start range function in applyArchivingOnStreamingJob
    val commitFile = FileUtils.getPath("test-archiver", jobId, mappingUrl.hashCode.toString, "commits", "0").toFile
    // Ensure the parent directories exist, if not, create them
    commitFile.getParentFile.mkdirs()
    val commitWriter = new PrintWriter(commitFile)
    commitWriter.write("v1\n{\"nextBatchWatermarkMs\":0}")
    commitWriter.close()

    // Find the relative path between the workspace folder and the file to be archived
    val relPath = FileUtils.getPath("").toAbsolutePath.relativize(FileUtils.getPath("test-archiver", "test.csv").toAbsolutePath)
    // The relative path is appended to the base archive folder so that the path of the original input file is preserved
    val finalArchivePath = FileUtils.getPath(ToFhirConfig.engineConfig.archiveFolder, relPath.toString)

    // Call archiving function
    fileStreamInputArchiver.applyArchivingOnStreamingJob(mockTaskExecution, mappingUrl)

    // Check whether archiving files exist
    finalArchivePath.toFile.exists() shouldBe true
    testCsvFile.exists() shouldBe false
    // Clean test directory
    org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath("test-archiver").toFile)
    org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(ToFhirConfig.engineConfig.archiveFolder).toFile)
  }
}
