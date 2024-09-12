package io.tofhir.test.engine.data.read

import io.tofhir.engine.config.ToFhirConfig
import io.tofhir.engine.data.read.FileDataSourceReader
import io.tofhir.engine.model.{FileSystemSource, FileSystemSourceSettings, SourceFileFormats}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.{contain, include}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.nio.file.Paths
import java.sql.Timestamp
import java.text.SimpleDateFormat
import scala.language.postfixOps

/**
 * Unit tests for the FileDataSourceReader class.
 * Tests the functionality of reading files in different formats.
 */
class FileDataSourceReaderTest extends AnyFlatSpec with BeforeAndAfterAll {

  /**
   * SparkSession used for the test cases.
   */
  val sparkSession: SparkSession = ToFhirConfig.sparkSession
  /**
   * Path to the directory containing test data for FileDataSourceReader.
   */
  val testDataFolderPath: String = Paths.get(getClass.getResource("/file-data-source-reader-test-data").toURI).toAbsolutePath.toString
  /**
   * Instance of FileDataSourceReader used to read data files during tests.
   */
  val fileDataSourceReader = new FileDataSourceReader(sparkSession)
  /**
   * Date format used for parsing and formatting date values in test cases.
   */
  val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  /**
   * Tests that the FileDataSourceReader correctly handles invalid input by throwing the appropriate exceptions.
   *
   * The test case covers two scenarios:
   * 1. Providing a file path instead of a directory for a streaming job should result in an IllegalArgumentException.
   * 2. Providing unsupported file formats or extensions should result in a NotImplementedError.
   *
   * The following configurations are used for the tests:
   * - `illegalArgumentSourceBinding`: A source binding with a 'file' path to test the directory requirement for streaming jobs.
   * - `unsupportedFileFormatSourceBinding`: A source binding with an unsupported file format to test the unsupported format handling.
   * - `unsupportedExtensionSourceBinding`: A source binding with an unsupported file extension to test the unsupported extension handling.
   * - `streamJobSourceSettings`: Mapping job source settings configured for a streaming job.
   * - `batchJobSourceSettings`: Mapping job source settings configured for a batch job.
   *
   * The test verifies the following:
   * 1. An IllegalArgumentException is thrown with the expected message when a file path is provided instead of a directory for a streaming job.
   * 2. A NotImplementedError is thrown for unsupported file formats and extensions, indicating that these cases are not yet implemented or handled.
   *
   */
  it should "throw IllegalArgumentException, NotImplementedError when necessary" in {
    // Folder including the test files belong to this test
    val folderPath = "/single-file-test"

    // Test case 1: Verify that providing a file path instead of a directory throws an IllegalArgumentException
    val fileName: String = "patients.csv"
    val illegalArgumentSourceBinding = FileSystemSource(path = fileName)
    val streamJobSourceSettings = FileSystemSourceSettings(name = "FileDataSourceReaderTest0", sourceUri = "test-uri", dataFolderPath = testDataFolderPath.concat(folderPath), asStream = true)
    val exception = intercept[IllegalArgumentException] {
      fileDataSourceReader.read(illegalArgumentSourceBinding, streamJobSourceSettings, Option.empty)
    }
    exception.getMessage should include(s"${fileName} is not a directory. For streaming job, you should provide a directory.")

    // Test case 2: Verify that unsupported file formats and extensions throw a NotImplementedError
    val unsupportedFileFormatSourceBinding = FileSystemSource(path = fileName, fileFormat = Some("UNSUPPORTED"))
    val unsupportedExtensionSourceBinding = FileSystemSource(path = "patients.UNSUPPORTED")
    val batchJobSourceSettings = streamJobSourceSettings.copy(asStream = false)
    assertThrows[NotImplementedError] {
      fileDataSourceReader.read(unsupportedFileFormatSourceBinding, batchJobSourceSettings, Option.empty)
    }
    assertThrows[NotImplementedError] {
      fileDataSourceReader.read(unsupportedExtensionSourceBinding, batchJobSourceSettings, Option.empty)
    }
  }

  /**
   * Tests that the FileDataSourceReader correctly reads data from CSV, TSV, and TXT_CSV files.
   *
   * This test verifies that the reader can handle different file formats and produce the expected results.
   * The test covers the following formats:
   * 1. CSV
   * 2. TSV
   * 3. TXT_CSV (Text file with CSV-like format)
   *
   * The test uses the following source binding configurations:
   * FileSystemSource(path = "patients.csv", fileFormat = None, options = Map("ignoreTrailingWhiteSpace" -> "true", "comment" -> "!"))
   * FileSystemSource(path = "patients.tsv", fileFormat = None, options = Map("ignoreTrailingWhiteSpace" -> "true", "comment" -> "!"))
   * FileSystemSource(path = "patients.txt", fileFormat = "txt-csv", options = Map("ignoreTrailingWhiteSpace" -> "true", "comment" -> "!"))
   *
   * The expected read result for all file formats:
   * +---+------+-------------------+----------------+--------------+
   * |pid|gender|          birthDate|deceasedDateTime|homePostalCode|
   * +---+------+-------------------+----------------+--------------+
   * | p1|  male|2000-05-10 00:00:00|            NULL|          NULL|
   * | p2|  male|1985-05-08 00:00:00|      2017-03-10|        G02547|
   * | p3|  male|1997-02-01 00:00:00|            NULL|          NULL|
   * | p4|  male|1999-06-05 00:00:00|            NULL|        H10564|
   * | p5|  male|1965-10-01 00:00:00|      2019-04-21|        G02547|
   * | p6|female|1991-03-01 00:00:00|            NULL|          NULL|
   * | p7|female|1972-10-25 00:00:00|            NULL|        V13135|
   * | p8|female|2010-01-10 00:00:00|            NULL|        Z54564|
   * | p9|female|1999-05-12 00:00:00|            NULL|          NULL|
   * |p10|female|2003-11-01 00:00:00|            NULL|          NULL|
   * +---+------+-------------------+----------------+--------------+
   *
   */
  it should "correctly read from CSV, TSV, and TXT_CSV files" in {
    // Folder containing the test files for this test
    val folderPath = "/single-file-test"

    // Expected values for validation
    val expectedRowNumber = 10
    val expectedColumns = Array("pid", "gender", "birthDate", "deceasedDateTime", "homePostalCode")
    val expectedFirstRow = Row("p1", "male", new Timestamp(dateFormat.parse("2000-05-10").getTime), null, null)
    val expectedLastRow = Row("p10", "female", new Timestamp(dateFormat.parse("2003-11-01").getTime), null, null)

    // A sequence of file names and their corresponding formats to be tested
    val sourceBindingConfigurations = Seq(
      ("patients.csv", None),
      ("patients.tsv", None),
      ("patients.txt", Some(SourceFileFormats.TXT_CSV))
    )

    // Spark options to test if options are working
    val sparkOptions = Map(
      "ignoreTrailingWhiteSpace" -> "true",
      "comment" -> "!"
    )

    // Loop through each source binding configuration to run the test
    val mappingJobSourceSettings = FileSystemSourceSettings(name = "FileDataSourceReaderTest1", sourceUri = "test-uri", dataFolderPath = testDataFolderPath.concat(folderPath))
    sourceBindingConfigurations.foreach { case (fileName, fileFormat) =>
      // Read the data using the reader and the defined settings
      val mappingSourceBinding = FileSystemSource(path = fileName, fileFormat = fileFormat, options = sparkOptions)
      val result: DataFrame = fileDataSourceReader.read(mappingSourceBinding, mappingJobSourceSettings, Option.empty)

      // Validate the result
      result.count() shouldBe expectedRowNumber
      result.columns shouldBe expectedColumns
      result.first() shouldBe expectedFirstRow
      result.collect().last shouldBe expectedLastRow
    }
  }

  /**
   * Tests that the FileDataSourceReader correctly reads multiple files from CSV and TXT_CSV folders.
   *
   * This test verifies that the reader can handle multiple files across different file formats
   * and produce the expected results. The test covers reading from folders containing:
   * 1. CSV files
   * 2. TXT_CSV (Text files with CSV-like format)
   *
   * The test uses the following source binding configurations:
   * FileSystemSource(path = "csv", fileFormat = Some(SourceFileFormats.CSV), options = Map("ignoreTrailingWhiteSpace" -> "true", "comment" -> "!"))
   * FileSystemSource(path = "txt-csv", fileFormat = Some(SourceFileFormats.TXT_CSV), options = Map("ignoreTrailingWhiteSpace" -> "true", "comment" -> "!"))
   *
   * The expected read result for both folder formats:
   * +---+------+-------------------+
   * |pid|gender|          birthDate|
   * +---+------+-------------------+
   * | p1|  male|2000-05-10 00:00:00|
   * | p2|  male|1985-05-08 00:00:00|
   * | p3|  male|1997-02-01 00:00:00|
   * | p4|  male|1999-06-05 00:00:00|
   * | p5|  male|1965-10-01 00:00:00|
   * | p6|female|1991-03-01 00:00:00|
   * | p7|female|1972-10-25 00:00:00|
   * | p8|female|2010-01-10 00:00:00|
   * | p9|female|1999-05-12 00:00:00|
   * +---+------+-------------------+
   * (Rows may appear in different groupings, with each file contributing a distinct set of 3 rows.)
   *
   */
  it should "correctly read multiple files from CSV, TXT_CSV folders" in {
    // Folder including the test folders belong to this test
    val folderPath = "/folder-test"

    // Expected values for validation
    val expectedRowNumber = 9
    val expectedColumns = Array("pid", "gender", "birthDate")
    val expectedRows = Set( // One row from each file
      Row("p1", "male", new Timestamp(dateFormat.parse("2000-05-10").getTime)),
      Row("p4", "male", new Timestamp(dateFormat.parse("1999-06-05").getTime)),
      Row("p7", "female", new Timestamp(dateFormat.parse("1972-10-25").getTime))
    )

    // A sequence of folder names and file format of the files to be selected
    val sourceBindingConfigurations = Seq(
      ("csv", Some(SourceFileFormats.CSV)),
      ("txt-csv", Some(SourceFileFormats.TXT_CSV))
    )
    // Spark options to test if options are working
    val sparkOptions = Map(
      "ignoreTrailingWhiteSpace" -> "true",
      "comment" -> "!"
    )

    // Loop through each source binding configuration to run the test
    val mappingJobSourceSettings = FileSystemSourceSettings(name = "FileDataSourceReaderTest2", sourceUri = "test-uri", dataFolderPath = testDataFolderPath.concat(folderPath))
    sourceBindingConfigurations.foreach { case (folderName, fileFormat) =>
      // Read the data using the reader and the defined settings
      val mappingSourceBinding = FileSystemSource(path = folderName, fileFormat = fileFormat, options = sparkOptions)
      val result: DataFrame = fileDataSourceReader.read(mappingSourceBinding, mappingJobSourceSettings, Option.empty)

      // Validate the result
      result.count() shouldBe expectedRowNumber
      result.columns shouldBe expectedColumns
      result.collect().toSet should contain allElementsOf expectedRows
    }
  }

  /**
   * Tests that the FileDataSourceReader correctly reads data from JSON and TXT_NDJSON files.
   *
   * This test verifies that the reader can handle different file formats and produce the expected results.
   * The test covers the following formats:
   * 1. JSON
   * 2. TXT_NDJSON (Text file with newline-delimited JSON format)
   *
   * The test uses the following source binding configurations:
   * FileSystemSource(path = "patients.json", fileFormat = None, options = Map("allowComments" -> "true"))
   * FileSystemSource(path = "patients-ndjson.txt", fileFormat = Some(SourceFileFormats.TXT_NDJSON), options = Map("allowComments" -> "true"))
   *
   * The expected read result is for both file formats:
   * +------------+----------------+------+--------------+---+
   * |  birthDate |deceasedDateTime|gender|homePostalCode|pid|
   * +------------+----------------+------+--------------+---+
   * |2000-05-10  |            NULL|  male|          NULL| p1|
   * |1985-05-08  |      2017-03-10|  male|        G02547| p2|
   * |1997-02     |            NULL|  male|          NULL| p3|
   * |1999-06-05  |            NULL|  male|        H10564| p4|
   * |1965-10-01  |      2019-04-21|  male|        G02547| p5|
   * |1991-03     |            NULL|female|          NULL| p6|
   * |1972-10-25  |            NULL|female|        V13135| p7|
   * |2010-01-10  |            NULL|female|        Z54564| p8|
   * |1999-05-12  |            NULL|female|          NULL| p9|
   * |2003-11     |            NULL|female|          NULL|p10|
   * +------------+----------------+------+--------------+---+
   * (Row order appears randomly due to 'distinct' option)
   */
  it should "correctly read from JSON and TXT-NDJSON files" in {
    // Folder including the test files
    val folderPath = "/single-file-test"

    // Define the expected values for validation (Note: Spark reads json columns in alphabetic order)
    val expectedRowNumber = 10
    val expectedColumns = Array("birthDate", "deceasedDateTime", "gender", "homePostalCode", "pid")
    val expectedFirstRow = Row("2000-05-10", null, "male", null, "p1")
    val expectedLastRow = Row("2003-11", null, "female", null, "p10")

    // Define the file names and their corresponding formats to be tested
    val sourceBindingConfigurations = Seq(
      ("patients.json", None),
      ("patients-ndjson.txt", Some(SourceFileFormats.TXT_NDJSON))
    )
    // Spark options to test if options are working
    val sparkOptions = Map(
      "allowComments" -> "true",
      "distinct" -> "true" // 'distinct' option randomly changes the order of the rows in the result.
    )

    // Loop through each source binding configuration to run the test
    val mappingJobSourceSettings = FileSystemSourceSettings(name = s"FileDataSourceReaderTest3", sourceUri = "test-uri", dataFolderPath = testDataFolderPath.concat(folderPath))
    sourceBindingConfigurations.foreach { case (fileName, fileFormat) =>
      // Define the source binding and settings for reading the file
      val mappingSourceBinding = FileSystemSource(path = fileName, fileFormat = fileFormat, options = sparkOptions)
      // Read the data from the specified file
      val result: DataFrame = fileDataSourceReader.read(mappingSourceBinding, mappingJobSourceSettings, Option.empty)

      // Validate the result
      result.count() shouldBe expectedRowNumber
      result.columns shouldBe expectedColumns
      // Check that the result contains the first and last row of the source data
      result.collect().contains(expectedFirstRow) shouldBe true
      result.collect().contains(expectedLastRow) shouldBe true
    }
  }

  /**
   * Tests that the FileDataSourceReader correctly reads multiple files from JSON and NDJSON folders.
   *
   * This test verifies that the reader can handle multiple files across different file formats
   * and produce the expected results. The test covers reading from folders containing:
   * 1. JSON (standard JSON files in the "json" folder)
   * 2. TXT_NDJSON (newline-delimited JSON files in the "txt-ndjson" folder)
   *
   * The test uses the following source binding configurations:
   * FileSystemSource(path = "json", fileFormat = Some(SourceFileFormats.JSON))
   * FileSystemSource(path = "txt-ndjson", fileFormat = Some(SourceFileFormats.TXT_NDJSON))
   *
   * The expected read result for both formats:
   * +----------+------+---+
   * | birthDate|gender|pid|
   * +----------+------+---+
   * |2000-05-10|  male| p1|
   * |1985-05-08|  male| p2|
   * |   1997-02|  male| p3|
   * |1999-06-05|  male| p4|
   * |1965-10-01|  male| p5|
   * |   1991-03|female| p6|
   * |1972-10-25|female| p7|
   * |2010-01-10|female| p8|
   * |1999-05-12|female| p9|
   * +----------+------+---+
   * (Rows may appear in different groupings, with each file contributing a distinct set of 3 rows.)
   */
  it should "correctly read multiple files from JSON and NDJSON folders" in {
    // Folder containing the test folders for JSON and NDJSON files
    val folderPath = "/folder-test"

    // Expected values for validation
    val expectedRowNumber = 9
    val expectedColumns = Array("birthDate", "gender", "pid")
    // Expected rows for validation, one row from each file
    val expectedRows = Set(
      Row("2000-05-10", "male", "p1"),
      Row("1999-06-05", "male", "p4"),
      Row("1972-10-25", "female", "p7")
    )

    // A sequence of folder names and file format of the files to be selected
    val sourceBindingConfigurations = Seq(
      ("json", Some(SourceFileFormats.JSON)),
      ("txt-ndjson", Some(SourceFileFormats.TXT_NDJSON))
    )

    // Loop through each source binding configuration to run the test
    val mappingJobSourceSettings = FileSystemSourceSettings(name = "FileDataSourceReaderTest4", sourceUri = "test-uri", dataFolderPath = testDataFolderPath.concat(folderPath))
    sourceBindingConfigurations.foreach { case (folderName, fileFormat) =>
      // Read the data using the reader and the defined settings
      val mappingSourceBinding = FileSystemSource(path = folderName, fileFormat = fileFormat)
      val result: DataFrame = fileDataSourceReader.read(mappingSourceBinding, mappingJobSourceSettings, Option.empty)

      // Validate the result
      result.count() shouldBe expectedRowNumber
      result.columns shouldBe expectedColumns
      result.collect().toSet should contain allElementsOf expectedRows
    }
  }

  /**
   * Tests that the FileDataSourceReader correctly reads data from a Parquet file.
   *
   * This test verifies that the reader can handle Parquet file format and produce the expected results.
   * The test covers the following format:
   * 1. PARQUET
   *
   * The test uses the following source binding configuration:
   * FileSystemSource(path = "patients.parquet", fileFormat = Some(SourceFileFormats.PARQUET))
   *
   * The expected read result for the Parquet file is:
   * +---+------+----------+-------------------+--------------+
   * |pid|gender| birthDate|deceasedDateTime  |homePostalCode|
   * +---+------+----------+-------------------+--------------+
   * | p1|  male|2000-05-10|               null|          null|
   * | p2|  male|1985-05-08|2017-03-10         |        G02547|
   * | p3|  male|1997-02-01|               null|          null|
   * | p4|  male|1999-06-05|               null|        H10564|
   * | p5|  male|1965-10-01|2019-04-21         |        G02547|
   * | p6|female|1991-03-01|               null|          null|
   * | p7|female|1972-10-25|               null|        V13135|
   * | p8|female|2010-01-10|               null|        Z54564|
   * | p9|female|1999-05-12|               null|          null|
   * |p10|female|2003-11   |               null|          null|
   * +---+------+----------+-------------------+--------------+
   *
   */
  it should "correctly read from Parquet file" in {
    // Folder including the test files
    val folderPath = "/single-file-test"

    // Define the expected values for validation
    val expectedRowNumber = 10
    val expectedColumns = Array("pid", "gender", "birthDate", "deceasedDateTime", "homePostalCode")
    val expectedFirstRow = Row("p1", "male", "2000-05-10", null, null)
    val expectedLastRow = Row("p10", "female", "2003-11", null, null)

    // Define the file name and its corresponding format for Parquet
    val sourceBindingConfigurations = Seq(
      ("patients.parquet", Some(SourceFileFormats.PARQUET))
    )

    // Loop through each source binding configuration to run the test
    val mappingJobSourceSettings = FileSystemSourceSettings(name = s"FileDataSourceReaderTest5", sourceUri = "test-uri", dataFolderPath = testDataFolderPath.concat(folderPath))
    sourceBindingConfigurations.foreach { case (fileName, fileFormat) =>
      // Define the source binding and settings for reading the file
      val mappingSourceBinding = FileSystemSource(path = fileName, fileFormat = fileFormat)
      // Read the data from the specified file
      val result: DataFrame = fileDataSourceReader.read(mappingSourceBinding, mappingJobSourceSettings, Option.empty)

      // Validate the result
      result.count() shouldBe expectedRowNumber
      result.columns shouldBe expectedColumns
      result.first() shouldBe expectedFirstRow
      result.collect().last shouldBe expectedLastRow
    }
  }

  /**
   * Tests that the FileDataSourceReader correctly reads data from Parquet files.
   *
   * This test verifies that the reader can handle Parquet file format and produce the expected results.
   * The test covers the following format:
   * 1. PARQUET
   *
   * The test uses the following source binding configuration:
   * FileSystemSource(path = "parquet", fileFormat = Some(SourceFileFormats.PARQUET))
   *
   * The expected read result for the Parquet files is:
   * +---+------+----------+
   * |pid|gender| birthDate|
   * +---+------+----------+
   * | p1|  male|2000-05-10|
   * | p2|  male|1985-05-08|
   * | p3|  male|1997-02-01|
   * | p4|  male|1999-06-05|
   * | p5|  male|1965-10-01|
   * | p6|female|1991-03-01|
   * | p7|female|1972-10-25|
   * | p8|female|2010-01-10|
   * | p9|female|1999-05-12|
   * +---+------+----------+
   * (Rows may appear in different order, grouped by each file.)
   */
  it should "correctly read multiple files from Parquet folders" in {
    // Folder including the test folders belonging to this test
    val folderPath = "/folder-test"

    // Expected values for validation
    val expectedRowNumber = 9
    val expectedColumns = Array("pid", "gender", "birthDate")
    // Expected rows for validation, one row from each file
    val expectedRows = Set(
      Row("p1", "male", "2000-05-10"),
      Row("p4", "male", "1999-06-05"),
      Row("p7", "female", "1972-10-25")
    )

    // A sequence of folder names and file format of the files to be selected
    val sourceBindingConfigurations = Seq(
      ("parquet", Some(SourceFileFormats.PARQUET))
    )

    // Loop through each source binding configuration to run the test
    val mappingJobSourceSettings = FileSystemSourceSettings(name = "FileDataSourceReaderTest6", sourceUri = "test-uri", dataFolderPath = testDataFolderPath.concat(folderPath))
    sourceBindingConfigurations.foreach { case (folderName, fileFormat) =>
      // Read the data using the reader and the defined settings
      val mappingSourceBinding = FileSystemSource(path = folderName, fileFormat = fileFormat)
      val result: DataFrame = fileDataSourceReader.read(mappingSourceBinding, mappingJobSourceSettings, Option.empty)

      // Validate the result
      result.count() shouldBe expectedRowNumber
      result.columns shouldBe expectedColumns
      result.collect().toSet should contain allElementsOf expectedRows
    }
  }

  /**
   * Tests that the FileDataSourceReader correctly reads multiple CSV, TSV, and TXT_CSV files from a zip file.
   *
   * This test verifies that the reader can handle multiple files inside a zip archive with different file formats
   * and produce the expected results. The test covers reading from a zip file containing:
   * 1. CSV files
   * 2. TSV files
   * 3. TXT_CSV (Text files with CSV format)
   *
   * The expected read result for all formats:
   * +---+------+-------------------+
   * |pid|gender|          birthDate|
   * +---+------+-------------------+
   * | p1|  male|2000-05-10 00:00:00|
   * | p2|  male|1985-05-08 00:00:00|
   * | p3|  male|1997-02-01 00:00:00|
   * | p4|  male|1999-06-05 00:00:00|
   * | p5|  male|1965-10-01 00:00:00|
   * | p6|female|1991-03-01 00:00:00|
   * | p7|female|1972-10-25 00:00:00|
   * | p8|female|2010-01-10 00:00:00|
   * | p9|female|1999-05-12 00:00:00|
   * +---+------+-------------------+
   * (Rows may appear in different order, grouped by each file.)
   *
   */
  it should "correctly read multiple CSV, TSV and TXT-CSV files from a zip archive" in {
    // Path to the zip file containing the test data
    val folderPath = "/zip-test"

    // Expected values for validation
    val expectedRowNumber = 9
    val expectedColumns = Array("pid", "gender", "birthDate")
    val expectedRows = Set(
      Row("p1", "male", new Timestamp(dateFormat.parse("2000-05-10").getTime)),
      Row("p4", "male", new Timestamp(dateFormat.parse("1999-06-05").getTime)),
      Row("p7", "female", new Timestamp(dateFormat.parse("1972-10-25").getTime))
    )

    // A sequence of file formats and their configuration for the test
    val sourceBindingConfigurations = Seq(
      ("csv.zip", Some(SourceFileFormats.CSV)),
      ("tsv.zip", Some(SourceFileFormats.TSV)),
      ("txt-csv.zip", Some(SourceFileFormats.TXT_CSV))
    )

    // Spark options to test if options are working
    val sparkOptions = Map(
      "ignoreTrailingWhiteSpace" -> "true",
      "comment" -> "!"
    )

    // Test the reading from the zip file using the source configurations
    val mappingJobSourceSettings = FileSystemSourceSettings(name = "FileDataSourceReaderTest7", sourceUri = "zip-file-uri", dataFolderPath = testDataFolderPath.concat(folderPath))
    sourceBindingConfigurations.foreach { case (zipFileName, fileFormat) =>
      // Read the data using the reader and the defined settings
      val mappingSourceBinding = FileSystemSource(path = zipFileName, fileFormat = fileFormat, options = sparkOptions)
      val result: DataFrame = fileDataSourceReader.read(mappingSourceBinding, mappingJobSourceSettings, Option.empty)

      // Validate the result
      result.count() shouldBe expectedRowNumber
      result.columns shouldBe expectedColumns
      result.collect().toSet should contain allElementsOf expectedRows
    }
  }

  /**
   * Tests that the FileDataSourceReader correctly reads data from JSON and TXT_NDJSON files inside a zip archive.
   *
   * This test verifies that the reader can handle different file formats (JSON, TXT_NDJSON) compressed in a zip file
   * and produce the expected results.
   *
   * The test covers the following formats inside a zip:
   * 1. JSON
   * 2. TXT_NDJSON (Text file with newline-delimited JSON format)
   *
   * The expected result for both file formats:
   * +------------+----------------+------+--------------+---+
   * |  birthDate |deceasedDateTime|gender|homePostalCode|pid|
   * +------------+----------------+------+--------------+---+
   * |2000-05-10  |            NULL|  male|          NULL| p1|
   * |1985-05-08  |      2017-03-10|  male|        G02547| p2|
   * |1997-02     |            NULL|  male|          NULL| p3|
   * |1999-06-05  |            NULL|  male|        H10564| p4|
   * |1965-10-01  |      2019-04-21|  male|        G02547| p5|
   * |1991-03     |            NULL|female|          NULL| p6|
   * |1972-10-25  |            NULL|female|        V13135| p7|
   * |2010-01-10  |            NULL|female|        Z54564| p8|
   * |1999-05-12  |            NULL|female|          NULL| p9|
   * |2003-11     |            NULL|female|          NULL|p10|
   * +------------+----------------+------+--------------+---+
   */
  it should "correctly read from JSON and TXT-NDJSON files inside a zip archive" in {

    // Path to the zip file containing the test files
    val folderPath = "/zip-test"

    // Expected values for validation
    val expectedRowNumber = 9
    val expectedColumns = Array("birthDate", "gender", "pid")
    // Expected rows for validation, one row from each file
    val expectedRows = Set(
      Row("2000-05-10", "male", "p1"),
      Row("1999-06-05", "male", "p4"),
      Row("1972-10-25", "female", "p7")
    )

    // Define the zip file names and their corresponding formats to be tested
    val sourceBindingConfigurations = Seq(
      ("json.zip", Some(SourceFileFormats.JSON)), // JSON inside zip
      ("txt-ndjson.zip", Some(SourceFileFormats.TXT_NDJSON)) // Newline-delimited JSON inside zip
    )

    // Spark options for testing (e.g., allowing comments in the files)
    val sparkOptions = Map(
      "allowComments" -> "true"
    )

    // Loop through each zip file and perform the test
    val mappingJobSourceSettings = FileSystemSourceSettings(name = "FileDataSourceReaderTest8", sourceUri = "zip-uri", dataFolderPath = testDataFolderPath.concat(folderPath))

    sourceBindingConfigurations.foreach { case (zipFileName, fileFormat) =>

      // Define the source binding and read the data from the zip file
      val mappingSourceBinding = FileSystemSource(path = zipFileName, fileFormat = fileFormat, options = sparkOptions)
      val result: DataFrame = fileDataSourceReader.read(mappingSourceBinding, mappingJobSourceSettings, Option.empty)

      // Validate the result
      result.count() shouldBe expectedRowNumber
      result.columns shouldBe expectedColumns
      result.collect().toSet should contain allElementsOf expectedRows
    }
  }

}