package io.tofhir.engine.util

import com.fasterxml.jackson.dataformat.csv.{CsvMapper, CsvSchema}

import java.io.File
import scala.jdk.javaapi.CollectionConverters

object CsvUtil {

  /**
   * Read the CSV file from the given filePath and return a Sequence where each element is a Map[column_name -> value)
   * @param filePath  File path
   * @return
   */
  def readFromCSV(filePath: String): Seq[Map[String, String]] = {
      val csvFile = new File(filePath)
      val csvMapper = new CsvMapper()
      val csvSchema = CsvSchema.emptySchema().withHeader()

      val javaList: java.util.List[java.util.Map[String, String]] =
        csvMapper.readerFor(classOf[java.util.Map[String, String]]) // read each line into a Map[String, String]
          .`with`(csvSchema) // where the key of the map will be the column name according to the first (header) row
          .readValues(csvFile)
          .readAll() // Read all lines as a List of Map
      CollectionConverters.asScala(javaList)
        .toSeq // convert the outer List to Scala Seq
        .map(CollectionConverters.asScala(_).toMap) // convert each inner Java Map to Scala Map
  }

}
