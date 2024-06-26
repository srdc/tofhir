package io.tofhir.server.util

import com.typesafe.scalalogging.Logger
import io.tofhir.engine.Execution.actorSystem
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.util.FileUtils.FileExtensions
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.writePretty
import io.tofhir.engine.util.FhirMappingJobFormatter.formats
import io.tofhir.server.common.model.InternalError

import java.io.{File, FileWriter}
import java.nio.charset.StandardCharsets
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
   * Reads the given file into a JSON object
   * @param f
   * @return
   */
  def readFileIntoJson(f: File): JValue = {
    val source = Source.fromFile(f, StandardCharsets.UTF_8.name())
    val fileContent = try source.mkString finally source.close()
    parse(fileContent)
  }

  /**
   * Write seq content to a json file
   * @param f File to be written
   * @param content array of objects to be saved
   * @tparam K class that can be parsed to JSON and saved
   */
  def writeJsonContent[K](f: File, content: Seq[K]): Unit = {
    val writer = new FileWriter(f)
    try {
      writer.write(writePretty(content))
    } finally {
      writer.close()
    }
  }

  /**
   * Read the content of a json file and parse it to a sequence of objects
   *
   * @param f File to be read
   * @param c class type that can be parsed from JSON
   * @tparam K class that created from JSON
   * @return Sequence of objects
   */
  def readJsonContentAsObject[K](f: File)(implicit ev: scala.reflect.Manifest[K]): K = {
    val source = Source.fromFile(f, StandardCharsets.UTF_8.name())
    val fileContent = try source.mkString finally source.close()
    val parsed = parse(fileContent).extract[K]
    parsed
  }


  /**
   * Checks whether the given CSV file is a unit conversion file. A CSV file is a unit conversion file
   * if it includes a column called "conversion_function" in its first line.
   *
   * @param f CSV file
   * @return true if the file is a unit conversion file, false otherwise
   * */
  def isUnitConversionFile(f: File): Boolean = {
    val source = Source.fromFile(f, StandardCharsets.UTF_8.name())
    val firstLine = try source.getLines().toSeq.head finally source.close()
    firstLine.contains("conversion_function")
  }

  /**
   * Get folder if exists
   * @param path path of the folder
   * @return File object
   */
  def getFileIfExists(path: String): File = {
    val folder = new File(path)
    if (folder.exists()) {
      folder
    } else {
      throw InternalError("File not found.", s"$path file should exists.")
    }
  }

  /**
   * Check whether the file name matches with the entity id
   * @param entityId id of the entity e.g. job id, mapping id
   * @param file file to whose name will be checked
   * @param entityType type of the entity e.g. job, mapping
   * @return true if the file name matches with the entity id, false otherwise
   */
  def checkFileNameMatchesEntityId(entityId: String, file: File, entityType: String): Boolean = {
    if (!entityId.equals(file.getName.replace(FileExtensions.JSON.toString, ""))) {
      logger.warn(s"Discarding ${entityType} definition with id ${entityId} as it does not match with the file name ${file.getName}")
      false
    } else {
      true
    }
  }

  /**
   * Write string content to a file
   * @param file file to be written
   * @param content content to be written
   */
  def writeStringContentToFile(file: File, content: String): Unit = {
    val fileWriter = new FileWriter(file)
    fileWriter.write(content)
    fileWriter.close()
  }
}

