package io.tofhir.engine.util

import io.tofhir.engine.config.ToFhirConfig
import io.tofhir.engine.util.FileUtils.FileExtensions.FileExtensions

import java.io.File
import java.nio.file.{Path, Paths}

object FileUtils {

  /**
   * Given a list of paths, construct a single path by appending them from left to right by starting from
   * #ToFhirConfig.mappingJobFileContextPath which is used as the root context path for any kind of file access within
   * toFHIR.
   *
   * @param paths
   * @return
   */
  def getPath(path: String, paths: String*): Path = {
    val givenPath =
      if (paths.isEmpty) Paths.get(path)
      else Paths.get(
        Paths.get(path).normalize().toString,
        paths.map(p => Paths.get(p).normalize().toString): _*
      )
    val resultingPath = if (givenPath.isAbsolute) givenPath
    else Paths.get(
      ToFhirConfig.mappingJobFileContextPath,
      givenPath.toString)
    resultingPath.normalize()
  }

  /**
   * Get the list of JSON files (ending with .json) from the folder repository.
   *
   * @return
   */
  def getFilesFromFolder(folder: File, withExtension: FileExtensions, recursively: Boolean = true): Seq[File] = {
    if (folder.exists && folder.isDirectory) {
      val files = folder.listFiles().toList // List all available files in the given folder
      val jsonFiles = files.filter(_.getName.endsWith(withExtension.toString)) // Find the JSON files
      if (recursively) {
        val subFolders = files.filter(_.isDirectory)
        jsonFiles ++ subFolders.flatMap(f => getFilesFromFolder(f, withExtension))
      } else {
        jsonFiles
      }
    } else {
      throw new IllegalArgumentException(s"Given folder is not valid. Path: ${folder.getAbsolutePath}")
    }
  }

  object FileExtensions extends Enumeration {
    type FileExtensions = Value
    final val CSV = Value(".csv")
    final val JSON = Value(".json")
  }

}
