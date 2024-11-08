package io.tofhir.server.service.fhir

import io.tofhir.engine.util.FileUtils
import io.tofhir.server.model.FilePathNode
import io.tofhir.server.service.fhir.FileSystemTreeStructureService.FILE_LIMIT

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
    Files.isHidden(path) || fileName.startsWith(".") // Exclude hidden files and files starting with . (dot)
  }

  /**
   * Get the folder tree structure of the given base path
   * @param basePathString The base path to start with
   * @param includeFiles if false, only folders will be included in the tree
   * @return
   */
  def getFolderTreeStructure(basePathString: String, includeFiles: Boolean): FilePathNode = {
    var fileCount = 0 // Track total number of files processed

    val basePath = FileUtils.getPath(basePathString)
    if(!basePath.toAbsolutePath.normalize().startsWith(FileUtils.getPath("").toAbsolutePath)) {
      throw new IllegalArgumentException("The given path is outside the root context path.")
    }

    def buildNode(path: Path): FilePathNode = {
      if (fileCount >= FILE_LIMIT) {
        throw new IllegalStateException(s"File limit exceeded. The maximum number of files to process is $FILE_LIMIT.")
      }

      val children = if (Files.isDirectory(path)) {
        val dirStream = Files.list(path)
        try {
          dirStream.iterator().asScala
            .filter(p => includeFiles || Files.isDirectory(p))
            .filterNot(shouldBeExcluded)  // Filter out hidden files, folders
            .map { p =>
              fileCount += 1
              buildNode(p)
            }
            .toList
        } finally {
          dirStream.close() // Ensures the stream is closed after usage
        }
      } else {
        List.empty[FilePathNode]
      }
      FilePathNode(path.getFileName.toString, Files.isDirectory(path), basePath.relativize(path).toString.replaceAll("\\\\", "/"), children)
    }
    buildNode(basePath)
  }

}

object FileSystemTreeStructureService {
  val FILE_LIMIT = 10000 // Maximum number of files to process
}