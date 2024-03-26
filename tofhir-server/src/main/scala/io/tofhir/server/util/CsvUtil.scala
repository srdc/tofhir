package io.tofhir.server.util

import akka.stream.scaladsl.{Concat, FileIO, Framing, Sink, Source}
import akka.util.ByteString
import io.tofhir.engine.Execution.actorSystem
import io.tofhir.engine.Execution.actorSystem.dispatcher

import java.io.File
import java.nio.file.StandardOpenOption
import scala.concurrent.Future

object CsvUtil {

  /**
   * Get the content of a CSV file as a Source of ByteString
   * @param file CSV file
   * @param pageNumber number of the page to read
   * @param pageSize size of the page
   * @return Future[(Source[ByteString, Any], Long)] where the first element is the Source of ByteString and the second element is the total number of rows in the CSV file
   */
  def getPaginatedCsvContent(file: File, pageNumber: Int, pageSize: Int): Future[(Source[ByteString, Any], Long)] = {
    val totalRecordsFuture: Future[Long] = FileIO.fromPath(file.toPath)
      .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024, allowTruncation = true))
      .drop(1)
      .runFold(0L)((count, _) => count + 1)

    totalRecordsFuture.map { totalRecords =>
      val start = (pageNumber - 1) * pageSize
      val end = start + pageSize
      val csvFile = FileIO.fromPath(file.toPath)

      val headerSource = csvFile
        // Filter out \r characters to avoid issues with Windows line endings
        .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024, allowTruncation = true))
        .take(1) // Take only the first line for header

      val content = csvFile
        .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024, allowTruncation = true))
        .drop(start + 1) // +1 to skip the header
        .take(pageSize)

      val source = Source.combine(headerSource, content)(Concat(_))
        //.filterNot(x => x == ByteString("\r"))
        .intersperse(ByteString("\n"))
        .map { bs =>
          // If the ByteString ends with a CR character, remove it
          if (bs.endsWith(ByteString("\r"))) {
            bs.dropRight(1)
          } else {
            bs
          }
        }
      (source, totalRecords)
    }
  }

  /**
   * Given a CSV file and content, write the content to the file starting from the specified page number
   * @param file CSV file to write to
   * @param content Source of ByteString to write
   * @param pageNumber Page number to start writing from
   * @param pageSize Page size
   * @return
   */
  def writePaginatedCsvContent(file: File, content: Source[ByteString, Any], pageNumber: Int, pageSize: Int): Future[Unit] = {
    val start = (pageNumber - 1) * pageSize + 1 // +1 to skip the header
    val end = start + pageSize

    // Convert the content to a list of strings
    val contentFuture: Future[List[String]] = content
      .map(_.utf8String)
      .runWith(Sink.seq)
      .map(_.toList)

    // Read the existing CSV file into a list of strings
    val existingContentFuture: Future[List[String]] = FileIO.fromPath(file.toPath)
      .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024, allowTruncation = true))
      .map(_.utf8String)
      .runWith(Sink.seq)
      .map(_.toList)

    for {
      newContent <- contentFuture
      existingContent <- existingContentFuture
    } yield {
      // Replace the rows in the existing CSV file list with the new content
      val updatedContent = existingContent.patch(start, newContent, end - start)
      // Write the updated list back to the CSV file
      val byteSource = Source(updatedContent).map(s => ByteString(s + "\n"))
      byteSource.runWith(FileIO.toPath(file.toPath, Set(StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)))
    }
  }

}
