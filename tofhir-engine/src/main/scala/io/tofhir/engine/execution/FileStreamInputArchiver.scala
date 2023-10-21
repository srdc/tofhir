package io.tofhir.engine.execution

import com.typesafe.scalalogging.Logger
import io.tofhir.engine.config.ToFhirConfig
import io.tofhir.engine.execution.FileStreamInputArchiver._
import io.tofhir.engine.model.ArchiveModes.ArchiveModes
import io.tofhir.engine.model.{ArchiveModes, FhirMappingJobExecution, FileSystemSource, FileSystemSourceSettings}
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
  def applyArchivingOnStreamingJob(taskExecution: FhirMappingJobExecution, mappingUrl: String): Unit = {
    // Get the commit file directory for this execution
    val checkPointDirectory: String = taskExecution.getCheckpointDirectory(mappingUrl)
    val commitFileDirectory: File = Paths.get(checkPointDirectory, "commits").toFile

    // There won't be any file during the initialization or after checkpoints are cleared
    if (commitFileDirectory.listFiles().nonEmpty) {
      // Apply archiving for the files as of the last processed offset until the last unprocessed offset
      val lastProcessedOffset: Int = processedOffsets.getOrElseUpdate(getOffsetKey(taskExecution.id, mappingUrl), -1)
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
            processedOffsets.put(getOffsetKey(taskExecution.id, mappingUrl), sourceFileName)
          }
        })
    }
  }

  /**
   * Resets the offset to -1 for the given execution and mapping url so that the archiving starts from scratch
   *
   * @param execution
   * @param mappingUrl
   */
  def resetOffset(execution: FhirMappingJobExecution, mappingUrl: String): Unit = {
    processedOffsets.put(getOffsetKey(execution.id, mappingUrl), -1)
  }
}

object FileStreamInputArchiver {
  private val logger: Logger = Logger(this.getClass)

  def applyArchivingOnBatchJob(execution: FhirMappingJobExecution): Unit = {
    var paths: Seq[String] = Seq.empty
    // Putting the archiving logic for the batch file inside within a try block so that it would not affect the caller in case of any exception.
    // That means archiving works in best-effort mode.
    try {
      val archiveMode: ArchiveModes = execution.job.dataProcessingSettings.archiveMode
      if (archiveMode != ArchiveModes.OFF) {
        val fileSystemSourceSettings = execution.job.sourceSettings.head._2.asInstanceOf[FileSystemSourceSettings]
        // get data folder path from data source settings
        val dataFolderPath = fileSystemSourceSettings.dataFolderPath

        // Get paths of the input files referred by the mapping tasks
        paths = execution.mappingTasks.flatMap(mapping => {
          mapping.sourceContext.flatMap(fhirMappingSourceContextMap => {
            fhirMappingSourceContextMap._2 match {
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
      case t:Throwable => logger.warn(s"Failed to apply archiving for job: ${execution.job.id}, execution: ${execution.id}")
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
      logger.info(s"Archived input file successfully: ${inputFile.getAbsoluteFile}")
    }
  }


  /**
   * Parses the source file corresponding to the commit number. Returns the
   *
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

  /**
   * Computes hash code of the concatenation of the execution id and the mapping url
   *
   * @param executionId
   * @param mappingUrl
   * @return
   */
  private def getOffsetKey(executionId: String, mappingUrl: String): String = {
    (executionId + mappingUrl).hashCode.toString
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
      val finalArchivePath = FileUtils.getPath(ToFhirConfig.engineConfig.archiveFolder, relPath.toString)
      val archiveFile: File = new File(finalArchivePath.toString)

      // create parent directories if not exists
      archiveFile.getParentFile.mkdirs()

      // Check if the parent folder already contains a file with the same name. Delete, if yes
      val parentDirectory: File = archiveFile.getParentFile
      parentDirectory.listFiles().find(f => f.getName.contentEquals(archiveFile.getName)) match {
        case None =>
        case Some(file) =>
          file.delete()
          logger.debug(s"File with the same name exists in the archive, deleting it. File: ${file.getAbsoluteFile}")
      }

      // We need to check whether the input file still exists.
      // It might not exist in the following scenario: It has already been archived, the system is restarted and archiving starts from offset 0 and
      // tries to rearchive the file.
      if (file.exists()) {
        Files.move(file.toPath, archiveFile.toPath)
      } else {
        logger.debug(s"File to archive does not exist. File: ${file.getAbsoluteFile}")
      }

    } catch {
      case t: Throwable => logger.warn(s"Failed to archive the file: ${file.getAbsolutePath}", t.getMessage)
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
    // Get executions with streaming queries and apply
    val executions = runningJobRegistry.getRunningExecutionsWithCompleteMetadata()
      .filter(execution => execution.isStreaming())
    executions.foreach(execution => {
      if (execution.isStreaming()) {
        execution.getStreamingQueryMap().keys.foreach(mappingUrl => {
          archiver.applyArchivingOnStreamingJob(execution, mappingUrl)
        })
      }
    })
  }


}
