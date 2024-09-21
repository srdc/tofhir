package io.tofhir.server.util

import com.typesafe.scalalogging.Logger
import io.onfhir.api.Resource
import io.onfhir.api.util.IOUtil
import io.onfhir.util.OnFhirZipInputStream
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.util.FileUtils.FileExtensions
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.writePretty
import io.tofhir.engine.util.FhirMappingJobFormatter.formats
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.common.model.{BadRequest, InternalError}
import org.apache.commons.io.input.BOMInputStream
import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods

import java.io.{File, FileWriter, InputStreamReader, Reader}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.zip.ZipEntry
import scala.collection.mutable
import scala.concurrent.Future
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
   *
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
   *
   * @param f       File to be written
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
   *
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
   *
   * @param entityId   id of the entity e.g. job id, mapping id
   * @param file       file to whose name will be checked
   * @param entityType type of the entity e.g. job, mapping
   * @return true if the file name matches with the entity id, false otherwise
   */
  def checkFileNameMatchesEntityId(entityId: String, file: File, entityType: String): Boolean = {
    if (!entityId.equals(IOUtil.removeFileExtension(file.getName))) {
      logger.warn(s"Discarding $entityType definition with id $entityId as it does not match with the file name ${file.getName}")
      false
    } else {
      true
    }
  }

  /**
   * Processes a ZIP file containing FHIR structure definitions and extracts the resources.
   *
   * This method reads the ZIP file, parses each JSON resource, and collects them into a sequence of
   * `Resource` objects.
   *
   * @param zipFilePath The path to the ZIP file to be processed.
   * @return A Future containing a sequence of `Resource` objects extracted from the ZIP file.
   */
  def processZipFile(zipFilePath: Path): Future[Seq[Resource]] = Future {
    /**
     * Parses a JSON resource from the provided reader.
     *
     * This method attempts to parse a JSON resource from the given `Reader`. If the path does not end with ".json",
     * or if the JSON parsing fails, appropriate exceptions are thrown.
     *
     * @param reader The reader to read the JSON data from.
     * @param path   The file path from which the resource is being read.
     * @return The parsed `Resource` object from the JSON data.
     * @throws InternalError If there is a problem parsing the JSON data from the path.
     * @throws BadRequest    If the file path does not end with ".json".
     */
    def parseResource(reader: Reader, path: String): Resource = {
      if (path.endsWith(".json"))
        try {
          JsonMethods.parse(reader).asInstanceOf[JObject]
        }
        catch {
          case e: Exception =>
            throw InternalError("JSON parsing problem", s"Cannot parse resource from path $path!", Some(e))
        }
      else
        throw BadRequest("Invalid JSON!", s"Cannot read resource from path $path, it should be JSON file!")
    }

    val zipStream = new OnFhirZipInputStream(Files.newInputStream(zipFilePath))
    val resources: mutable.ListBuffer[Resource] = new mutable.ListBuffer[Resource]
    var zipEntry: ZipEntry = zipStream.getNextEntry

    while (zipEntry != null) {
      val reader = new InputStreamReader(BOMInputStream.builder.setInputStream(zipStream).get(), "UTF-8")
      resources.append(parseResource(reader, zipEntry.getName))
      zipStream.closeEntry()
      zipEntry = zipStream.getNextEntry
    }
    resources.toSeq
  }

  /**
   * Write string content to a file
   *
   * @param file    file to be written
   * @param content content to be written
   */
  def writeStringContentToFile(file: File, content: String): Unit = {
    val fileWriter = new FileWriter(file)
    fileWriter.write(content)
    fileWriter.close()
  }

  /**
   * Gets the File object for the given entityId (jobId, mappingId, schemaId).
   *
   * @param repositoryPath (Path to the job repository, schema repository or mapping repository)
   * @param projectId The id of the toFHIR project.
   * @param entityId The id of the job, mapping or schema.
   * @return
   */
  def getFileForEntityWithinProject(repositoryPath: String, projectId: String, entityId: String): File = {
    val file: File = FileUtils.getPath(repositoryPath, projectId, s"$entityId${FileExtensions.JSON}").toFile
    // If the project folder does not exist, create it
    if (!file.getParentFile.exists()) {
      file.getParentFile.mkdir()
    }
    file
  }
}

