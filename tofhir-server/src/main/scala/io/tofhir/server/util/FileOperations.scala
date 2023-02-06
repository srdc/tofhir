package io.tofhir.server.util

import java.io.{File, FileWriter}
import java.nio.charset.StandardCharsets
import akka.stream.scaladsl.FileIO
import akka.util.ByteString
import io.onfhir.util.JsonFormatter._
import io.tofhir.engine.Execution.actorSystem
import io.tofhir.server.model.InternalError
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.util.{Failure, Success}
import io.tofhir.engine.Execution.actorSystem.dispatcher
import org.json4s.jackson.Serialization.writePretty

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
   * Write content to a file by using akka streams
   * @param file File to be saved
   * @param content Content of the file
   */
  def saveFileContent(file: File, content: akka.stream.scaladsl.Source[ByteString, Any]): Unit = {
    content.runWith(FileIO.toPath(file.toPath)).onComplete({
      case Success(_) =>
      case Failure(e) =>
        throw InternalError("Error while writing file.", e.getMessage)
    })
  }
}
