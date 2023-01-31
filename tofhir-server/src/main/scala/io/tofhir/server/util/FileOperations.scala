package io.tofhir.server.util

import java.io.File
import java.nio.charset.StandardCharsets

import io.onfhir.util.JsonFormatter._
import io.tofhir.server.model.InternalError
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.io.Source

object FileOperations {
  /**
   * Read the content of a json file and parse it to a sequence of objects
   *
   * @param f File to be read
   * @param c class type that can be parsed from JSON
   * @tparam K class that created from JSON
   * @return Sequence of objects
   */
  def readJsonContent[K](f: File, c: Class[K])(implicit ev: scala.reflect.Manifest[K]): Seq[K] = {
    val source = Source.fromFile(f, StandardCharsets.UTF_8.name())
    val fileContent = try source.mkString finally source.close()
    val parsed = parse(fileContent).extract[Seq[K]]
    parsed
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
}
