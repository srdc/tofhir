package io.tofhir.test.engine.execution

import java.io.{File, PrintWriter}
import java.nio.file.{Path, Paths}

/**
 * Utility class that is mainly used to access and modify checkpoint-related files managed by Spark. This class serves for the purpose of being the
 * single point where Spark-related logic is implemented.
 */
object SparkUtil {

  /**
   * Gets the source file keeping the names of the input files. This file is managed by Spark inside the checkpoint directory.
   * The current implementation of Spark adds a root folder with name "0", keeping the other source files.
   *
   * @param sourceDirectory Root source directory created by Spark. This directory is supposed to keep job and task-specific source files.
   * @param sourceFileName  Actual file that is contained the source directory
   * @return
   */
  def getSourceFile(sourceDirectory: Path, sourceFileName: String): File = {
    Paths.get(sourceDirectory.toString, "sources", "0", sourceFileName).toFile
  }

  /**
   * Gets the commit file corresponding to the sources. This file is managed by Spark inside the checkpoint directory.
   *
   * @param commitDirectory Root commit directory created by Spark. This directory is supposed to keep job and task-specific commit files.
   * @param commitFileName  Actual file that is contained the commit directory
   * @return
   */
  def getCommitFile(commitDirectory: Path, commitFileName: String): File = {
    Paths.get(commitDirectory.toString, "commits", commitFileName).toFile
  }


  /**
   * Write csv path to source file
   *
   * @param sourceWriter Writer for source file
   * @param testCsvFile  Selected csv file
   */
  def writeToSourceFile(sourceWriter: PrintWriter, testCsvFile: File): Unit = {
    sourceWriter.write(s"{\"path\":\"${testCsvFile.getAbsolutePath.replace("\\", "\\\\")}\"}\n")
  }
}
