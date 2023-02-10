package io.tofhir.server.util

import com.typesafe.scalalogging.Logger

import java.io.{File, FileWriter}
import java.nio.charset.StandardCharsets
import io.onfhir.util.JsonFormatter._
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.model.Project
import io.tofhir.server.service.project.ProjectFolderRepository
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

import scala.io.Source

object FileOperations {

  private val logger: Logger = Logger(this.getClass)

  /**
   * Read the content of a json file and parse it to a sequence of objects
   *
   * @param f File to be read
   * @param c class type that can be parsed from JSON
   * @tparam K class that created from JSON
   * @return Sequence of objects
   */
  def readJsonContent[K](f: File)(implicit ev: scala.reflect.Manifest[K]): Seq[K] = {
    val source = Source.fromFile(f, StandardCharsets.UTF_8.name())
    val fileContent = try source.mkString finally source.close()
    val parsed = parse(fileContent).extract[Seq[K]]
    parsed
  }

  /**
   * Returns the entities in the repository by reading them from the projects metadata file.
   *
   * @return projects in the repository
   * */
  def getMetadata[T](repositoryFolderPath: String, metadataFile: String)(implicit ev: scala.reflect.Manifest[T]): Seq[T] = {
    val file = FileUtils.findFileByName(repositoryFolderPath + File.separatorChar, metadataFile)
    file match {
      case Some(f) =>
        FileOperations.readJsonContent[T](f)
      case None => {
        // when projects metadata file does not exist, create it
        logger.debug(s"There does not exist a metadata file: $metadataFile. Creating it...")
        val file: File = new File(ProjectFolderRepository.PROJECTS_JSON)
        file.createNewFile()
        // initialize projects metadata file with empty array
        val fw = new FileWriter(file)
        try fw.write("[]") finally fw.close()
        Seq.empty
      }
    }
  }

  /**
   * Updates the projects metadata file with the given projects.
   *
   * @param projects projects
   * */
  def updateMetadata[T](repositoryFolderPath: String, metadataFile: String, entities: Seq[T]) = {
    val file = new File(repositoryFolderPath + File.separatorChar, metadataFile)
    // if entity metadata file does not exist, create it
    if (!file.exists()) {
      logger.debug(s"There does not exist a metadata file: $metadataFile to update. Creating it...")
      file.createNewFile()
    }
    // write entities metadata to the file
    val fw = new FileWriter(file)
    try fw.write(Serialization.writePretty(entities)) finally fw.close()
  }
}
