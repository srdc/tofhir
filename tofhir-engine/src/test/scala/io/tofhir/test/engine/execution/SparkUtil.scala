package io.tofhir.test.engine.execution

import io.tofhir.engine.config.ToFhirConfig
import io.tofhir.engine.util.FileUtils

import java.io.{File, PrintWriter}

object SparkUtil {

  /**
   * Get source file from test-archiver directory.
   * @param jobId Job id of the execution.
   * @param mappingUrl Selected mapping url.
   * @param fileName source file name
   * @return Return source file
   */
  def getSourceFileFromTestArchiver(jobId: String, mappingUrl: String, fileName: String): File = {
    FileUtils.getPath("test-archiver", jobId, mappingUrl.hashCode.toString, "sources", "0", fileName).toFile
  }

  /**
   * Get source file from spark directory.
   * @param jobId Job id of the execution.
   * @param mappingUrl Selected mapping url.
   * @param fileName source file name
   * @return Return source file
   */
  def getSourceFileFromSparkArchiver(jobId: String, mappingUrl: String, fileName: String): File = {
    FileUtils.getPath(ToFhirConfig.sparkCheckpointDirectory, jobId, mappingUrl.hashCode.toString, "sources", "0", fileName).toFile
  }

  /**
   * Get commit file from test-archiver directory.
   * @param jobId Job id of the execution.
   * @param mappingUrl Selected mapping url.
   * @param fileName commit file name
   * @return Return commit file
   */
  def getCommitFileFromTestArchiver(jobId: String, mappingUrl: String, fileName: String): File = {
    FileUtils.getPath("test-archiver", jobId, mappingUrl.hashCode.toString, "commit", "0", fileName).toFile
  }

  /**
   * Write csv path to source file
   * @param sourceWriter Writer for source file
   * @param testCsvFile Selected csv file
   */
  def writeToSourceFile(sourceWriter: PrintWriter, testCsvFile: File): Unit = {
    sourceWriter.write(s"{\"path\":\"${testCsvFile.getAbsolutePath.replace("\\", "\\\\")}\"}\n")
  }
}
