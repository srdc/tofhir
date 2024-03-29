package io.tofhir.server.util

import akka.stream.scaladsl.{Concat, FileIO, Framing, Sink, Source}
import akka.util.ByteString
import io.tofhir.engine.Execution.actorSystem
import io.tofhir.engine.Execution.actorSystem.dispatcher

import java.io.File
import java.nio.file.StandardOpenOption
import scala.concurrent.Future
import com.opencsv.CSVParserBuilder

object CsvUtil {

  /**
   * Get the total number of rows in a file
   * @param file file to count the rows
   * @return Future[Long] total number of rows in the file
   */
  private def getTotalRows(file: File): Future[Long] = {
    FileIO.fromPath(file.toPath)
      .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024, allowTruncation = true))
      // filter out empty lines
      .filterNot(_.isEmpty)
      .drop(1)
      .runFold(0L)((count, _) => count + 1)
  }

  /**
   * Overwrite headers string to the first line of the CSV file
   * Adjust the rows to match the new headers if necessary e.g.
   * - if a column removed, remove the column from the rows
   * - if a new column name recognized, add the column to the rows with a default value (<column_name>)
   * - order of the columns are preserved according to the newHHeaders
   * @param file CSV file
   * @param newHeaders Headers to write to the file
   * @return
   */
  def writeCsvHeaders(file: File, newHeaders: Seq[String]): Future[Unit] = {
    // Create a CSVParser to handle double quotes
    val parser = new CSVParserBuilder().withSeparator(',').withQuoteChar('"').build()

    // Read the existing CSV file into a list of list where each map is a row
    val existingContentFuture = FileIO.fromPath(file.toPath)
      .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024, allowTruncation = true))
      .map(_.utf8String)
      .filterNot(_.isEmpty)
      .runWith(Sink.seq)
      .map { lines =>
        val oldHeaders = parser.parseLine(lines.head.trim)
        lines.tail.map { line =>
          oldHeaders.zip(parser.parseLine(line.trim)).toSeq
        }

      }

    // here existingContent looks like
    // [
    //   [ "header1" -> "value1", "header2" -> "value2" ],
    //   [ "header1" -> "value3", "header2" -> "value4" ]
    // ]
    existingContentFuture.map { existingContent =>
      // Create a new list of lists where each list is a row and the first element is the header
      val updatedContent = existingContent.map { row =>
        newHeaders.map { header =>
          header -> row.find(_._1 == header).map(_._2).getOrElse(s"<$header>")
        }
      }

      //Convert the list of lists back to a CSV format
      val csvContent = (newHeaders.mkString(",") +: updatedContent.map(_.map(x => s"\"${x._2}\"").mkString(","))).map(ByteString(_))
      // Write the updated CSV content back to the file
      Source(csvContent).intersperse(ByteString("\n"))
        .runWith(FileIO.toPath(file.toPath, Set(StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)))
    }
  }

  /**
   * Get the content of a CSV file as a Source of ByteString
   * @param file CSV file
   * @param pageNumber number of the page to read
   * @param pageSize size of the page
   * @return Future[(Source[ByteString, Any], Long)] where the first element is the Source of ByteString and the second element is the total number of rows in the CSV file
   */
  def getPaginatedCsvContent(file: File, pageNumber: Int, pageSize: Int): Future[(Source[ByteString, Any], Long)] = {
    val totalRecordsFuture: Future[Long] = this.getTotalRows(file)

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
   * After writing, return the total number of rows in the file
   * @param file CSV file to write to
   * @param content Source of ByteString to write
   * @param pageNumber Page number to start writing from
   * @param pageSize Page size
   * @return Future[Long] Future of total number of rows in the CSV file
   */
  def writeCsvAndReturnRowNumber(file: File, content: Source[ByteString, Any], pageNumber: Int, pageSize: Int): Future[Long] = {
    val start = (pageNumber - 1) * pageSize + 1 // +1 to skip the header
    val end = start + pageSize

    // Convert the content to a list of strings
    val contentFuture: Future[List[String]] = content
      .map(_.utf8String)
      .filterNot(_.isEmpty)
      .runWith(Sink.seq)
      .map(_.toList)

    // Read the existing CSV file into a list of strings
    val existingContentFuture: Future[List[String]] = FileIO.fromPath(file.toPath)
      .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024, allowTruncation = true))
      .map(_.utf8String)
      .filterNot(_.isEmpty)
      .runWith(Sink.seq)
      .map(_.toList)

    Future.sequence(List(contentFuture, existingContentFuture)).flatMap {
      case List(newContent, existingContent) =>
        // Replace the rows in the existing CSV file list with the new content
        val updatedContent = existingContent.patch(start, newContent, end - start)
        // Write the updated list back to the CSV file
        val byteSource = Source(updatedContent).map(s => ByteString(s + "\n")).filterNot(_.isEmpty)
        byteSource.runWith(FileIO.toPath(file.toPath, Set(StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)))
          .flatMap(
            _ => getTotalRows(file)
          )
    }
  }

}
