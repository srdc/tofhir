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
    val resultingPath = if (givenPath.isAbsolute) givenPath
    else Paths.get(
      ToFhirConfig.engineConfig.mappingJobFileContextPath,
      givenPath.toString)
    resultingPath.normalize()
  }

  /**
   * Find the file within the given directory by its type.
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

  /**
   * Creates a file name to be used in folder/file based entity management.
   * Removes all non-alphanumeric characters from the given name followed by a dash and given identifier
   *
   * @param id   Identifier of the entity
   * @param name Name of the entity
   * @return
   */
  def getFileName(id: String, name: String): String = {
    s"${name.replaceAll("[\\W_]+", "") + "-" + id}"
  }

  /**
   * Extracts the identifier of an entity from the file name.
   *
   * @param fileName File name like "name-id.StructureDefinition.json"
   * @return
   */
  def getId(fileName: String): String = {
    fileName.substring(fileName.indexOf("-") + 1).split("\\.")(0)
  }

  /**
   * Returns the path of the parent file of the specified file
   * @param path File of which parent's path to be returned
   * @return
   */
  def getParentFilePath(path: String): String = {
    new File(path).getAbsoluteFile.getParent
  }

  object FileExtensions extends Enumeration {
    type FileExtensions = Value
    final val StructureDefinition = Value(".StructureDefinition")
    final val CSV = Value(".csv")
    final val JSON = Value(".json")
  }

}
