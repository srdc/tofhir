package io.tofhir.engine.execution.processing

import com.typesafe.scalalogging.Logger
import io.tofhir.engine.config.ToFhirConfig
import io.tofhir.engine.execution.RunningJobRegistry
import io.tofhir.engine.execution.processing.FileStreamInputArchiver._
import io.tofhir.engine.model.ArchiveModes.ArchiveModes
import io.tofhir.engine.model.{ArchiveModes, FhirMappingJobExecution, FileSystemSource}
import io.tofhir.engine.util.{FileUtils, SparkUtil}

import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.util.concurrent.ConcurrentHashMap
import java.util.{Timer, TimerTask}
import scala.jdk.CollectionConverters._

/**
 * This class manages archiving of input files after mappings are executed in the file. It periodically runs an archiving thread, which checks the configured checkpoint directory for streaming jobs.
 * The checkpoint directory has a hierarchical structure such that it contains subdirectories keeping checkpoints for each job and mapping tasks included in the job
 * e.g. <checkpointDir> / <jobId> / <hash of mappingTaskName>. Each such folder further contains "commits", "offsets", and "sources" folders created by Spark. Spark creates a commit file
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
  // Keeps the last processed offsets for each streaming mapping task. The offsets correspond to the names of commit files.
  // Spark creates commit files with names as increasing numbers e.g. 0,1,2,...
  val processedOffsets: scala.collection.concurrent.Map[String, Int] = new ConcurrentHashMap[String, Int]().asScala

  def startStreamingArchiveTask(): Unit = {
    val timer: Timer = new Timer()
    timer.schedule(new StreamingArchiverTask(this, runningJobRegistry), 0, ToFhirConfig.engineConfig.streamArchivingFrequency)
  }

  /**
   * Applies archiving for the given execution.
   *
   * @param taskExecution
   */
  def applyArchivingOnStreamingJob(taskExecution: FhirMappingJobExecution, mappingTaskName: String): Unit = {
    val archiveMode: ArchiveModes = taskExecution.archiveMode
    if (archiveMode != ArchiveModes.OFF) {
      // Get the commit file directory for this execution
      val commitDirectory: File = new File(taskExecution.getCommitDirectory(mappingTaskName))

      // Get the sources file directory for this execution
      val sourcesDirectory: String = taskExecution.getSourceDirectory(mappingTaskName)

      // There won't be any file (with name as an integer) during the initialization or after checkpoints are cleared
      if (commitDirectory.listFiles().exists(file => file.isFile && !file.getName.contains("."))) {
        // Apply archiving for the files as of the last processed offset until the last unprocessed offset
        val lastProcessedOffset: Int = processedOffsets.getOrElseUpdate(getOffsetKey(taskExecution.id, mappingTaskName), -1)
        val lastOffsetSet: Int = SparkUtil.getLastCommitOffset(commitDirectory)

        Range.inclusive(lastProcessedOffset + 1, lastOffsetSet) // +1 for skipping the last processed offset
          .foreach(sourceFileName => {
            // Extract the actual input files
            val inputFiles: Seq[File] = SparkUtil.getInputFiles(Paths.get(sourcesDirectory, sourceFileName.toString).toFile)
            if (inputFiles.nonEmpty) {
              inputFiles.foreach(inputFile => {
                processArchiveMode(inputFile, archiveMode)
              })
              processedOffsets.put(getOffsetKey(taskExecution.id, mappingTaskName), sourceFileName)
            }
          })
      }
    }
  }

  /**
   * Resets the offset to -1 for the given execution and mappingTask name so that the archiving starts from scratch
   *
   * @param execution
   * @param mappingTaskName
   */
  def resetOffset(execution: FhirMappingJobExecution, mappingTaskName: String): Unit = {
    processedOffsets.put(getOffsetKey(execution.id, mappingTaskName), -1)
  }
}

object FileStreamInputArchiver {
  private val logger: Logger = Logger(this.getClass)

  def applyArchivingOnBatchJob(execution: FhirMappingJobExecution): Unit = {
    var paths: Seq[String] = Seq.empty
    // Putting the archiving logic for the batch job inside within a try block so that it would not affect the caller in case of any exception.
    // That means archiving works in best-effort mode.
    try {
      val archiveMode: ArchiveModes = execution.archiveMode
      if (archiveMode != ArchiveModes.OFF) {
        // get data folder path from data source settings
        val dataFolderPath = FileUtils.getPath(execution.fileSystemSourceDataFolderPath.get).toString

        // Get paths of the input files referred by the mapping tasks
        paths = execution.mappingTasks.flatMap(mappingTask => {
          mappingTask.sourceBinding.flatMap(mappingSourceBinding => {
            mappingSourceBinding._2 match {
              case fileSystemSource: FileSystemSource => Some(fileSystemSource.path)
              case _ => None
            }
          })
        })
        paths.foreach(relativePath => {
          val file = Paths.get(dataFolderPath, relativePath).toFile
          processArchiveMode(file.getAbsoluteFile, archiveMode)
        })
      }
    } catch {
      case t:Throwable => logger.warn(s"Failed to apply archiving for job: ${execution.jobId}, execution: ${execution.id}")
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
      logger.info(s"Deleted input file successfully: ${inputFile.getAbsoluteFile}")
    } else {
      moveSourceFileToArchive(inputFile)
    }
  }

  /**
   * Computes hash code of the concatenation of the execution id and the mappingTask name
   *
   * @param executionId
   * @param mappingTaskName
   * @return
   */
  private def getOffsetKey(executionId: String, mappingTaskName: String): String = {
    (executionId + mappingTaskName).hashCode.toString
  }

  /**
   * Move file from given path to given archive path.
   *
   * @param file File with absolute path
   */
  private def moveSourceFileToArchive(file: File): Unit = {
    try {
      // Find the relative path between the workspace folder and the file to be archived
      val relPath = FileUtils.getPath("").toAbsolutePath.relativize(file.toPath)
      // The relative path is appended to the base archive folder so that the path of the original input file is preserved
      val finalArchivePath = Paths.get(ToFhirConfig.engineConfig.archiveFolder, relPath.toString)
      val archiveFile: File = new File(finalArchivePath.toString)

      // create parent directories if not exists
      archiveFile.getParentFile.mkdirs()

      if (file.exists()) {
        // use StandardCopyOption.REPLACE_EXISTING to replace the existing one
        Files.move(file.toPath, archiveFile.toPath, StandardCopyOption.REPLACE_EXISTING)
        logger.info(s"Archived file: ${file.getAbsolutePath}")
      } else {
        logger.debug(s"File to archive does not exist. File: ${file.getAbsoluteFile}")
      }

    } catch {
      case t: Throwable => logger.warn(s"Failed to archive the file: ${file.getAbsolutePath}. Reason: ${t.getMessage}", t)
    }
  }
}

/**
 * Task to run periodically. It fetches the running executions and applies archiving for each
 *
 * @param archiver           Archiver to apply the archiving
 * @param runningJobRegistry Running job registry to fetch the running executions
 */
class StreamingArchiverTask(archiver: FileStreamInputArchiver, runningJobRegistry: RunningJobRegistry) extends TimerTask {
  override def run(): Unit = {
    // Get executions with streaming queries and file system sources and apply
    val executions = runningJobRegistry.getRunningExecutionsWithCompleteMetadata()
      .filter(execution => execution.isStreamingJob && execution.fileSystemSourceDataFolderPath.nonEmpty)
    executions.foreach(execution => {
      execution.getStreamingQueryMap().keys.foreach(mappingTaskName => {
        archiver.applyArchivingOnStreamingJob(execution, mappingTaskName)
      })
    })
  }
}
