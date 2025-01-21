package io.tofhir.engine.util

import io.onfhir.api.util.IOUtil
import io.tofhir.engine.config.ToFhirConfig

import java.io.File
import java.nio.file.{Path, Paths}

object FileUtils {

  /**
   * Constructs a single path by appending the given paths from left to right. If the initial path is relative,
   * it is appended to the root context path defined by [[ToFhirConfig.engineConfig.contextPath]].
   * This method is used for file access within toFHIR.
   *
   * @param path  The initial path to start with.
   * @param paths Additional paths to be appended to the initial path.
   * @return The constructed path.
   */
  def getPath(path: String, paths: String*): Path = {
    val givenPath =
      if (paths.isEmpty) Paths.get(path)
      else Paths.get(
        Paths.get(path).normalize().toString,
        paths.map(p => Paths.get(p).normalize().toString): _*
      )
    val resultingPath =
      if (givenPath.isAbsolute) givenPath
      else Paths.get(
        ToFhirConfig.engineConfig.contextPath,
        givenPath.toString)
    resultingPath.normalize()
  }

  /**
   * Find the file within the given directory by its type.
   *
   * @param repoPath
   * @param name
   * @return
   */
  def findFileByName(repoPath: String, name: String): Option[File] = {
    val repoFile = FileUtils.getPath(repoPath).toFile
    val allFiles = IOUtil.getFilesFromFolder(repoFile, recursively = true, ignoreHidden = true, withExtension = Some(FileExtensions.JSON.toString))
    val filteredFiles = allFiles.filter(f => {
      f.getName
        .toLowerCase.equals(name.toLowerCase)
    })
    if (filteredFiles.size > 1) throw new IllegalStateException(s"There are ${filteredFiles.size} definition files with the same name/rootPath!")
    filteredFiles.headOption
  }

  object FileExtensions extends Enumeration {
    type FileExtensions = Value
    final val CSV = Value(".csv")
    final val JSON = Value(".json")
  }

}
