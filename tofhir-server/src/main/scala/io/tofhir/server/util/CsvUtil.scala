package io.tofhir.server.util

import akka.stream.scaladsl.{Concat, FileIO, Flow, Framing, Sink, Source}
import akka.util.ByteString
import com.opencsv.CSVParserBuilder
import io.tofhir.engine.Execution.actorSystem
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.server.common.model.InternalError
import io.tofhir.server.model.csv.CsvHeader

import java.io.File
import java.nio.file.StandardOpenOption
import scala.concurrent.Future

object CsvUtil {

  /**
   * Flow to split a ByteString into lines
   * The maximum length of a line is set to 1024 characters
   * We cannot set an unlimited frame length because it could potentially lead to memory issues.
   * If the data stream contains a very long line (or a line without a delimiter),
   * it could cause the application to run out of memory while trying to buffer the entire line.
   * allowTruncation indicates that we don't require an explicit line ending even for the last message
   */
  private val lineDelimiterFlow: Flow[ByteString, ByteString, _] = Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024, allowTruncation = true)

  /**
   * Flow to remove carriage returns from a ByteString
   * \r characters are removed to avoid issues with Windows line endings
   */
  private val removeCarriageReturns: Flow[ByteString, ByteString, _] = Flow[ByteString].map { line =>
    val lineStr = line.utf8String // convert ByteString to String
    val modifiedLineStr = lineStr.replaceAll("\r", "") // remove \r characters
    ByteString(modifiedLineStr) // convert the modified String back to ByteString
  }

  /**
   * Get the total number of rows in a file
   *
   * @param file file to count the rows
   * @return Future[Long] total number of rows in the file
   */
  private def getTotalRows(file: File): Future[Long] = {
    FileIO.fromPath(file.toPath)
      .via(lineDelimiterFlow) // split the file content by lines
      .filterNot(_.isEmpty) // filter out empty lines
      .drop(1) // skip first line which is the header
      .runFold(0L)((count, _) => count + 1) // count the number of lines
  }

  /**
   * Overwrite headers string to the first line of the CSV file
   * Adjust the rows to match the new headers if necessary e.g.
   * - if a column name is changed, change headers of the rows with new name
   * - if a column removed, remove the column from the rows
   * - if a new column name recognized, add the column to the rows with a default value (<column_name>)
   * - order of the columns are preserved according to the newHHeaders
   * @param file CSV file
   * @param newHeaders Headers to write to the file
   * @return
   */
  def writeCsvHeaders(file: File, newHeaders: Seq[CsvHeader]): Future[Unit] = {
    // Used external CSV Parser library to handle double quotes in the CSV file
    val parser = new CSVParserBuilder().withSeparator(',').withQuoteChar('"').build()

    // Read the existing CSV file into a list of list where each list represents a row
    val existingContentFuture = FileIO.fromPath(file.toPath)
      .via(lineDelimiterFlow) // split the file content by lines
      .map(_.utf8String) // convert ByteString to String
      .filterNot(_.isEmpty) // filter out empty lines
      .runWith(Sink.seq)
      .map { lines =>
        if (lines.nonEmpty) { // check if csv is empty
          val oldHeaders = parser.parseLine(lines.head.trim) // parse the first line as headers
          lines.tail.map { line => // parse the rest of the lines as rows
            // for each row this returns a list of tuples where the first element in tuple is the header
            oldHeaders.zip(parser.parseLine(line.trim)).toSeq // e.g. Seq(("header1", "value1"), ("header2", "value2"))
          }
        } else {
          Seq.empty
        }
      }


    // here an example existingContent looks like:
    // header1,  header2,  header3
    // value1,   value2,   value3
    // value4,   value5,   value6
    //
    // an example newHeaders looks like:
    // Seq(
    //  CsvHeader(currentName = "header2", previousName = "header2"),
    //  CsvHeader(currentName = "headerChanged", previousName = "header3"),
    //  CsvHeader(currentName = "header4", previousName = "header4")  // previousName may be any placeholder
    // )
    // "header1" is deleted,
    // "header3" changed to "headerChanged"
    // "header4" is added
    existingContentFuture.map { existingContent =>
      // Create a new list of lists where each list is a row and the first element in the tuples is the header
      val updatedContent = existingContent.map { row => // iterate each row
        newHeaders.map { header => // iterate each new header
          // If the current name is different than the previously saved name, update the header of tuples
          val key = if (header.currentName != header.previousName) header.currentName else header.previousName
          // if the header already exists or header name is changed, use its value (header2 -> value2, headerChanged -> value3)
          // otherwise, use a default value (header4 -> <header4>)
          key -> row.find(_._1 == header.previousName).map(_._2).getOrElse(s"<${header.currentName}>")
        }
      }

      // here an example updatedContent looks like:
      // header2,  headerChanged,  header4
      // value2,   value3,         <header4>
      // value5,   value6,         <header4>

      // Extract the new names from CsvHeader array
      val newHeaderNames = newHeaders.map(_.currentName)
      // create a header line by joining the new headers with a comma
      // create row content by using the second element of the tuple and joining them with a comma
      // finally merge the header and row content with a comma
      val csvContent = (newHeaderNames.mkString(",") +: updatedContent.map(_.map(x => s"\"${x._2}\"").mkString(","))).map(ByteString(_))
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
      // calculate the start and end index of the CSV file
      val start = (pageNumber - 1) * pageSize
      val csvFile = FileIO.fromPath(file.toPath)

      val headerSource = csvFile
        .via(removeCarriageReturns) // remove out \r characters to avoid issues with Windows line endings
        .via(lineDelimiterFlow) // split the file content by lines
        .take(1) // Take only the first line for header

      val content = csvFile
        .via(removeCarriageReturns) // remove out \r characters to avoid issues with Windows line endings
        .via(lineDelimiterFlow) // split the file content by lines
        .drop(start + 1) // start from "start" value and +1 to skip the header
        .take(pageSize) // get page size number of lines

      // Combine the header and paginated content and return together with the total number of records
      val source = Source.combine(headerSource, content)(Concat(_)).intersperse(ByteString("\n"))
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

    // Convert the CSV content to a list of strings
    val contentFuture: Future[Seq[String]] = content
      .via(removeCarriageReturns) // remove out \r chars to avoid issues with Windows line endings
      .map(_.utf8String) // convert ByteString to String
      .filterNot(_.isEmpty) // filter out empty lines
      // collect values emitted from the stream into a collection, the collection is available through a Future
      // when the stream is run with Sink.seq (materialized), it will consume all the elements of the stream and return a Future[Seq[T]]
      .runWith(Sink.seq[String])

    // Convert the CSV content to a list of strings
    val existingContentFuture: Future[Seq[String]] = FileIO.fromPath(file.toPath)
      .via(removeCarriageReturns) // remove out \r chars to avoid issues with Windows line endings
      .via(lineDelimiterFlow) // split the file content by lines
      .map(_.utf8String) // convert ByteString to String
      .filterNot(_.isEmpty) // filter out empty lines
      .runWith(Sink.seq[String])

    Future.sequence(Seq(contentFuture, existingContentFuture)).flatMap {
      case Seq(newContent, existingContent) =>
        // Replace the rows in the existing CSV file with the new content starting from 'start' and number of 'pageSize' rows
        val updatedContent = existingContent.patch(start, newContent, pageSize)
        // Write the updated list back to the CSV file by
        // converting each line to ByteString and adding a newline character at the end
        // and then write line by line to the file
        val byteSource = Source(updatedContent).map(s => ByteString(s + "\n")).filterNot(_.isEmpty)
        byteSource.runWith(FileIO.toPath(file.toPath, Set(StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)))
          // flatMap is applied to the Future returned by runWith to get the total number of rows in the file in the end
          .flatMap(
            _ => getTotalRows(file)
          )
    }

  }


  /**
   * Overwrite the content of a CSV file with the given content
   * @param file file to be write
   * @param content content to write
   */
  def saveFileContent(file: File, content: akka.stream.scaladsl.Source[ByteString, Any]): Future[Unit] = {
    content
      .via(removeCarriageReturns) // remove out \r chars to avoid issues with Windows line endings
      .runWith(FileIO.toPath(file.toPath)) // use FileIO as a Sink to write the content to the file
      .map(_ => ()) // map the result of Future to Unit
      .recover(e => throw InternalError("Error while writing file.", e.getMessage))

  }
}
