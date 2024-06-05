package io.tofhir.server.service

import io.tofhir.engine.util.FileUtils
import io.tofhir.server.model.FilePathNode

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.IteratorHasAsScala

class FileSystemTreeStructureService {

  /**
   * Check if the given path should be excluded or not
   * @param path The path to check
   * @return
   */
  private def shouldBeExcluded(path: Path): Boolean = {
    val fileName = path.getFileName.toString
    Files.isHidden(path) || fileName.startsWith(".")
  }

  /**
   * Get the folder tree structure of the given base path
   * @param basePath The base path to start with
   * @param includeFiles if false, only folders will be included in the tree
   * @return
   */
  def getFolderTreeStructure(basePath: String, includeFiles: Boolean): FilePathNode = {
    def buildNode(path: Path): FilePathNode = {
      val children = if (Files.isDirectory(path)) {
        Files.list(path).iterator().asScala
          .filter(p => includeFiles || Files.isDirectory(p))
          .filterNot(p => shouldBeExcluded(p)) // Filter out hidden and specific folders
          .toList
          .map(buildNode)
      } else {
        List.empty[FilePathNode]
      }
      FilePathNode(path.getFileName.toString, Files.isDirectory(path), children)
    }
    val path = FileUtils.getPath(basePath)
    buildNode(path)
  }

}
