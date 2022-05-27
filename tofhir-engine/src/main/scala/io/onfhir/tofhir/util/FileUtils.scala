package io.onfhir.tofhir.util

import io.onfhir.tofhir.util.FileUtils.FileExtensions.FileExtensions

import java.io.File

object FileUtils {

  /**
   * Get the list of JSON files (ending with .json) from the folder repository.
   *
   * @return
   */
  def getFilesFromFolder(folder: File, withExtension: FileExtensions, recursively: Boolean = true): Seq[File] = {
    if (folder.exists && folder.isDirectory) {
      val files = folder.listFiles().toList // List all available files in the given folder
      val jsonFiles = files.filter(_.getName.endsWith(withExtension.toString)) // Find the JSON files
      if(recursively) {
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
