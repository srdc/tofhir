package io.tofhir.engine.util

import io.onfhir.api.util.IOUtil
import io.tofhir.engine.config.ToFhirConfig

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
    val allFiles = IOUtil.getFilesFromFolder(repoFile, withExtension = Some(FileExtensions.JSON.toString), recursively = Some(true))
    val filteredFiles = allFiles.filter(f => {
      f.getName
        .toLowerCase.equals(name.toLowerCase)
    })
    if (filteredFiles.size > 1) throw new IllegalStateException(s"There are ${filteredFiles.size} definition files with the same name/rootPath!")
    filteredFiles.headOption
  }

  object FileExtensions extends Enumeration {
    type FileExtensions = Value
    final val StructureDefinition = Value(".StructureDefinition")
    final val CSV = Value(".csv")
    final val JSON = Value(".json")
  }

}
