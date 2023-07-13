package io.tofhir.engine.util

import com.fasterxml.jackson.databind.MappingIterator
import com.fasterxml.jackson.dataformat.csv.{CsvMapper, CsvSchema}
import java.io.{File, FileInputStream, InputStreamReader}

import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.jdk.javaapi.CollectionConverters

object CsvUtil {

  /**
   * Read the CSV file from the given filePath and return a Sequence where each element is a Map[column_name -> value)
   * @param filePath  File path
   * @param encoding The encoding of CSV file whose default value is UTF-8
   * @return
   */
  def readFromCSV(filePath: String, encoding: String = "UTF-8"): Seq[Map[String, String]] = {
      val csvFile = new File(filePath)
      val csvMapper = new CsvMapper()
      val csvSchema = CsvSchema.emptySchema().withHeader()

      val mappingIterator:MappingIterator[java.util.Map[String, String]] =
        csvMapper.readerFor(classOf[java.util.Map[String, String]]) // read each line into a Map[String, String]
          .`with`(csvSchema) // where the key of the map will be the column name according to the first (header) row
          .readValues(new InputStreamReader(new FileInputStream(csvFile), encoding)) // read CSV with the given encoding

      val javaList: java.util.List[java.util.Map[String, String]] =  mappingIterator.readAll() // Read all lines as a List of Map

      CollectionConverters.asScala(javaList)
        .toSeq // convert the outer List to Scala Seq
        .map(CollectionConverters.asScala(_).toMap) // convert each inner Java Map to Scala Map
  }

  /**
   * Read the CSV file from the given filePath and return
   * - Sequence of column names in order
   * - a Sequence where each element is a Map[column_name -> value)
   * @param filePath File path
   * @return
   */
  def readFromCSVAndReturnWithColumnNames(filePath: String): (Seq[String], Seq[Map[String, String]]) = {
    val csvFile = new File(filePath)
    val csvMapper = new CsvMapper()
    val csvSchema = CsvSchema.emptySchema().withHeader()

    val mappingIterator: MappingIterator[java.util.Map[String, String]] =
      csvMapper.readerFor(classOf[java.util.Map[String, String]]) // read each line into a Map[String, String]
        .`with`(csvSchema) // where the key of the map will be the column name according to the first (header) row
        .readValues(csvFile)

    val javaList: java.util.List[java.util.Map[String, String]] = mappingIterator.readAll() // Read all lines as a List of Map

    val schema: CsvSchema = mappingIterator.getParser.getSchema.asInstanceOf[CsvSchema]
    val columns = schema.iterator.asScala.toSeq.map(_.getName)
    columns ->
      CollectionConverters.asScala(javaList)
        .toSeq // convert the outer List to Scala Seq
        .map(CollectionConverters.asScala(_).toMap) // convert each inner Java Map to Scala Map
  }

}
