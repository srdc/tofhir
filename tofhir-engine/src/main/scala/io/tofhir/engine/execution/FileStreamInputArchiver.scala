package io.tofhir.engine.execution

import io.tofhir.engine.config.ToFhirConfig
import io.tofhir.engine.model.ArchiveModes.ArchiveModes
import io.tofhir.engine.model.{ArchiveModes, FhirMappingJobExecution}
import io.tofhir.engine.util.FhirMappingJobFormatter.formats
import io.tofhir.engine.util.FileUtils
import org.json4s.jackson.JsonMethods

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.concurrent.ConcurrentHashMap
import java.util.{Timer, TimerTask}
import scala.io.Source
import scala.jdk.CollectionConverters._

/**
 * This class manages archiving of input files after mappings are executed in the file. It periodically runs an archiving thread, which checks the configured checkpoint directory for streaming jobs.
 * The checkpoint directory has a hierarchical structure such that it contains subdirectories keeping checkpoints for each job and mapping tasks included in the job
 * e.g. <checkpointDir> / <jobId> / <hash of mappingUrl>. Each such folder further contains "commits", "offsets", and "sources" folders created by Spark. Spark creates a commit file
 * after processing a file with a name as a number e.g. 0,1,2,...
 *
 * The "sources" folder contains corresponding files with the same name of commit files. These files in turn contain names of the actual files processed.
 *
 * This class periodically checks the commit files, identifies the input files corresponding to the commit and applies the archiving operation on them. The last processed offset is cached so that the
 * archiving won't start from scratch.
 *
 * @param runningJobRegistry Running job registry. It is used to fetch the list of active executions.
 */
class FileStreamInputArchiver(runningJobRegistry: RunningJobRegistry) {
  // Keeps the last processed offsets for each FhirMappingExecution. The offsets correspond to the names of commit files.
  // Spark creates commit files with names as increasing numbers e.g. 0,1,2,...
  val processedOffsets: scala.collection.concurrent.Map[String, Int] = new ConcurrentHashMap[String, Int]().asScala

  def startProcessorTask(): Unit = {
    val timer: Timer = new Timer()
    timer.schedule(new ProcessorTask(processedOffsets, runningJobRegistry), 0, 5000)
  }
}

/**
 * Task to apply archiving
 *
 * @param offsets            Last processed offsets for executions
 * @param runningJobRegistry Running job registry
 */
class ProcessorTask(offsets: scala.collection.concurrent.Map[String, Int], runningJobRegistry: RunningJobRegistry) extends TimerTask {
  override def run(): Unit = {
    // Get executions with streaming queries and apply
    val executions = runningJobRegistry.getRunningExecutionsWithCompleteMetadata()
      .filter(execution => execution.jobGroupIdOrStreamingQuery.get.isRight)
    executions.foreach(execution => {
      applyArchiving(execution)
    })
  }

  /**
   * Applies archiving for the given execution.
   *
   * @param taskExecution
   */
  private def applyArchiving(taskExecution: FhirMappingJobExecution): Unit = {
    // Get the commit file directory for this execution
    val checkPointDirectory: String = taskExecution.getCheckpointDirectory(taskExecution.mappingTasks.head.mappingRef)
    val commitFileDirectory: File = Paths.get(checkPointDirectory, "commits").toFile

    // There won't be any file during the initialization or after checkpoints are cleared
    if (commitFileDirectory.listFiles().nonEmpty) {
      // Apply archiving for the files as of the last processed offset until the last unprocessed offset
      val lastProcessedOffset: Int = offsets.getOrElseUpdate(taskExecution.id, -1)
      val lastOffsetSet: Int = getLastCommitOffset(commitFileDirectory)
      val archiveMode: ArchiveModes = taskExecution.job.dataProcessingSettings.archiveMode

      Range.inclusive(lastProcessedOffset + 1, lastOffsetSet) // +1 for skipping the last processed offset
        .foreach(sourceFileName => {
          // Extract the actual input files
          val inputFiles: Seq[File] = getInputFiles(Paths.get(checkPointDirectory, "sources", "0", sourceFileName.toString).toFile)
          if (inputFiles.nonEmpty) {
            inputFiles.foreach(inputFile => {
              processArchiveMode(inputFile, archiveMode)
            })
            offsets.put(taskExecution.id, sourceFileName)
          }
        })
    }
  }

  /**
   * Deletes or moves the input file based on the given archive mode
   *
   * @param inputFile   Input file passed by the user
   * @param archiveMode Archive mode
   */
  private def processArchiveMode(inputFile: File, archiveMode: ArchiveModes): Unit = {
    if (archiveMode == ArchiveModes.DELETE) {
      inputFile.delete()
    } else {
      moveSourceFileToArchive(inputFile)
    }
  }

  /**
   * Move file from given path to given archive path
   *
   * @param file
   */
  private def moveSourceFileToArchive(file: File): Unit = {
    // Find the relative path between the workspace folder and the file to be archived
    val relPath = FileUtils.getPath("").toAbsolutePath.relativize(file.toPath)
    // The relative path is appended to the base archive folder so that the path of the original input file is preserved
    val finalArchivePath = FileUtils.getPath(ToFhirConfig.engineConfig.archiveFolder, relPath.toString)
    val archiveFile: File = new File(finalArchivePath.toString)

    // create parent directories if not exists
    archiveFile.getParentFile.mkdirs()
    // move file to archive folder
    Files.move(file.toPath, archiveFile.toPath)
  }

  /**
   * Parses the source file corresponding to the commit number. Returns the
   * @param sourceFile The source file contains the names of the input files
   * @return
   */
  private def getInputFiles(sourceFile: File): Seq[File] = {
    val source = Source.fromFile(sourceFile)

    // Iterate through the lines in the file
    val sourceFiles: Seq[File] = source.getLines()
      .flatMap(line => {
        // Some lines do not contain the desired information
        try {
          // Source name is specified in the "path" field of a json object
          val path: String = (JsonMethods.parse(line) \ "path").extract[String]
          Some(new File(path.substring("file:///".length)))

        } catch {
          case _: Throwable => None
        }
      })
      .toSeq
    source.close()
    sourceFiles
  }

  /**
   * Finds the latest commit file generated by the Spark. It is an integer
   *
   * @param commitFileDirectory Directory containing the commit files
   * @return
   */
  private def getLastCommitOffset(commitFileDirectory: File): Int = {
    commitFileDirectory
      .listFiles()
      .filter(file => file.isFile && !file.getName.contains("."))
      .map(file => file.getName.toInt)
      .max
  }
}
