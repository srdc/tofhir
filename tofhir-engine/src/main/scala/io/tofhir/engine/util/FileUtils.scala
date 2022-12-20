package io.tofhir.engine.util

import io.tofhir.engine.config.ToFhirConfig

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
      ToFhirConfig.engineConfig.mappingJobFileContextPath,
      givenPath.toString)
    resultingPath.normalize()
  }

  object FileExtensions extends Enumeration {
    type FileExtensions = Value
    final val CSV = Value(".csv")
    final val JSON = Value(".json")
  }

}
