package io.tofhir.test.engine.execution

import io.tofhir.engine.config.ToFhirConfig
import io.tofhir.engine.util.FileUtils

import java.io.{File, PrintWriter}

object SparkUtil {

  def getSourceFile(jobId: String, mappingUrl: String, fileName: String): File = {
    FileUtils.getPath("test-archiver", jobId, mappingUrl.hashCode.toString, "sources", "0", fileName).toFile
  }

  def getCommitFile(jobId: String, mappingUrl: String, fileName: String): File = {
    FileUtils.getPath("test-archiver", jobId, mappingUrl.hashCode.toString, "commit", "0", fileName).toFile
  }

  def writeToSourceFile(sourceWriter: PrintWriter, testCsvFile: File): Unit = {
    sourceWriter.write(s"{\"path\":\"${testCsvFile.getAbsolutePath.replace("\\", "\\\\")}\"}\n")
  }

  /**
   * Initialize needed spark files.
   * @param jobId Job id of the execution.
   * @param mappingUrl Selected mapping url.
   * @return Return test csv file
   */
  def initializeSparkFiles(jobId: String, mappingUrl: String): File = {
    // Create a source file to refer location of test.csv
    val sourceFile = FileUtils.getPath(ToFhirConfig.sparkCheckpointDirectory, jobId, mappingUrl.hashCode.toString, "sources", "0", "0").toFile
    // Path of test.csv
    val testCsvFile = FileUtils.getPath(ToFhirConfig.sparkCheckpointDirectory, "test.csv").toFile
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
    val commitFile = FileUtils.getPath(ToFhirConfig.sparkCheckpointDirectory, jobId, mappingUrl.hashCode.toString, "commits", "0").toFile
    // Ensure the parent directories exist, if not, create them
    commitFile.getParentFile.mkdirs()
    val commitWriter = new PrintWriter(commitFile)
    commitWriter.write("v1\n{\"nextBatchWatermarkMs\":0}")
    commitWriter.close()

    testCsvFile
  }
}
