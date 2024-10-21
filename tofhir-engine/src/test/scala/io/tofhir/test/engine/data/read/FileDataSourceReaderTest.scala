package io.tofhir.test.engine.data.read

import io.tofhir.engine.config.ToFhirConfig
import io.tofhir.engine.data.read.FileDataSourceReader
import io.tofhir.engine.model.{FileSystemSource, FileSystemSourceSettings, SourceContentTypes}
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
   * 2. Providing unsupported content types should result in a NotImplementedError.
   *
   * The following configurations are used for the tests:
   * - `illegalArgumentSourceBinding`: A source binding with a 'file' path to test the directory requirement for streaming jobs.
   * - `unsupportedContentTypeSourceBinding`: A source binding with an unsupported content type to test the unsupported format handling.
   * - `streamJobSourceSettings`: Mapping job source settings configured for a streaming job.
   * - `batchJobSourceSettings`: Mapping job source settings configured for a batch job.
   *
   * The test verifies the following:
   * 1. An IllegalArgumentException is thrown with the expected message when a file path is provided instead of a directory for a streaming job.
   * 2. A NotImplementedError is thrown for unsupported content types, indicating that these cases are not yet implemented or handled.
   *
   */
  it should "throw IllegalArgumentException, NotImplementedError when necessary" in {
    // Folder including the test files belong to this test
    val folderPath = "/single-file-test"

    // Test case 1: Verify that providing a file path instead of a directory throws an IllegalArgumentException
    val fileName: String = "patients.csv"
    val illegalArgumentSourceBinding = FileSystemSource(path = fileName, contentType = SourceContentTypes.CSV)
    val streamJobSourceSettings = FileSystemSourceSettings(name = "FileDataSourceReaderTest0", sourceUri = "test-uri", dataFolderPath = testDataFolderPath.concat(folderPath), asStream = true)
    val exception = intercept[IllegalArgumentException] {
      fileDataSourceReader.read(illegalArgumentSourceBinding, streamJobSourceSettings, Option.empty)
    }
    exception.getMessage should include(s"${fileName} is not a directory. For streaming job, you should provide a directory.")

    // Test case 2: Verify that unsupported content types throw a NotImplementedError
    val unsupportedContentTypeSourceBinding = FileSystemSource(path = fileName, contentType = "UNSUPPORTED")
    val batchJobSourceSettings = streamJobSourceSettings.copy(asStream = false)
    assertThrows[NotImplementedError] {
      fileDataSourceReader.read(unsupportedContentTypeSourceBinding, batchJobSourceSettings, Option.empty)
    }
  }

  /**
   * Tests that the FileDataSourceReader correctly reads data from CSV, TSV, and TXT(csv) file.
   *
   * This test verifies that the reader can handle different content types and produce the expected results.
   * The test covers the following formats:
   * 1. CSV
   * 2. TSV
   * 3. TXT(csv) (Text file with CSV content type)
   *
   * The test uses the following source binding configurations:
   * FileSystemSource(path = "patients.csv", contentType = SourceContentTypes.CSV, options = Map("ignoreTrailingWhiteSpace" -> "true", "comment" -> "!"))
   * FileSystemSource(path = "patients.tsv", contentType = SourceContentTypes.TSV, options = Map("ignoreTrailingWhiteSpace" -> "true", "comment" -> "!"))
   * FileSystemSource(path = "patients.txt", contentType = SourceContentTypes.CSV, options = Map("ignoreTrailingWhiteSpace" -> "true", "comment" -> "!"))
   *
   * The expected read result for all content types:
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
  it should "correctly read from CSV, TSV, and TXT(csv) files" in {
    // Folder containing the test files for this test
    val folderPath = "/single-file-test"

    // Expected values for validation
    val expectedRowNumber = 10
    val expectedColumns = Array("pid", "gender", "birthDate", "deceasedDateTime", "homePostalCode")
    val expectedFirstRow = Row("p1", "male", new Timestamp(dateFormat.parse("2000-05-10").getTime), null, null)
    val expectedLastRow = Row("p10", "female", new Timestamp(dateFormat.parse("2003-11-01").getTime), null, null)

    // A sequence of file names and their corresponding formats to be tested
    val sourceBindingConfigurations = Seq(
      ("patients.csv", SourceContentTypes.CSV),
      ("patients.tsv", SourceContentTypes.TSV),
      ("patients.txt", SourceContentTypes.CSV)
    )

    // Spark options to test if options are working
    val sparkOptions = Map(
      "ignoreTrailingWhiteSpace" -> "true",
      "comment" -> "!"
    )

    // Loop through each source binding configuration to run the test
    val mappingJobSourceSettings = FileSystemSourceSettings(name = "FileDataSourceReaderTest1", sourceUri = "test-uri", dataFolderPath = testDataFolderPath.concat(folderPath))
    sourceBindingConfigurations.foreach { case (fileName, contentType) =>
      // Read the data using the reader and the defined settings
      val mappingSourceBinding = FileSystemSource(path = fileName, contentType = contentType, options = sparkOptions)
      val result: DataFrame = fileDataSourceReader.read(mappingSourceBinding, mappingJobSourceSettings, Option.empty)

      // Validate the result
      result.count() shouldBe expectedRowNumber
      result.columns shouldBe expectedColumns
      result.first() shouldBe expectedFirstRow
      result.collect().last shouldBe expectedLastRow
    }
  }

  it should "correctly read from limited and limited and random records from a CSV file" in {
    // Folder containing the test files for this test
    val folderPath = "/single-file-test"

    // Expected values for validation
    val expectedFirstRow = Row("p1", "male", new Timestamp(dateFormat.parse("2000-05-10").getTime), null, null)

    // Spark options to test if options are working
    val sparkOptions = Map(
      "ignoreTrailingWhiteSpace" -> "true",
      "comment" -> "!"
    )

    val mappingJobSourceSettings = FileSystemSourceSettings(name = "FileDataSourceReaderTest1", sourceUri = "test-uri", dataFolderPath = testDataFolderPath.concat(folderPath))
    val mappingSourceBinding = FileSystemSource(path = "patients.csv", contentType = SourceContentTypes.CSV, options = sparkOptions)
    val limitedResult: DataFrame = fileDataSourceReader.read(mappingSourceBinding, mappingJobSourceSettings, schema = Option.empty).limit(4)
    limitedResult.count() shouldBe 4
    limitedResult.first() shouldBe expectedFirstRow
  }

  /**
   * Tests that the FileDataSourceReader correctly reads multiple files from CSV and TXT(csv) folders.
   *
   * This test verifies that the reader can handle multiple files across different content types
   * and produce the expected results. The test covers reading from folders containing:
   * 1. CSV files
   * 2. TXT(csv) (Text files with csv content type)
   *
   * The test uses the following source binding configurations:
   * FileSystemSource(path = "csv", contentType = SourceContentTypes.CSV, options = Map("ignoreTrailingWhiteSpace" -> "true", "comment" -> "!"))
   * FileSystemSource(path = "txt-csv", contentType = SourceContentTypes.CSV, options = Map("ignoreTrailingWhiteSpace" -> "true", "comment" -> "!"))
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
  it should "correctly read multiple files from CSV, TXT(csv) folders" in {
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

    // A sequence of folder names and content types of the files to be selected
    val sourceBindingConfigurations = Seq(
      ("csv", SourceContentTypes.CSV),
      ("txt-csv", SourceContentTypes.CSV)
    )
    // Spark options to test if options are working
    val sparkOptions = Map(
      "ignoreTrailingWhiteSpace" -> "true",
      "comment" -> "!"
    )

    // Loop through each source binding configuration to run the test
    val mappingJobSourceSettings = FileSystemSourceSettings(name = "FileDataSourceReaderTest2", sourceUri = "test-uri", dataFolderPath = testDataFolderPath.concat(folderPath))
    sourceBindingConfigurations.foreach { case (folderName, contentType) =>
      // Read the data using the reader and the defined settings
      val mappingSourceBinding = FileSystemSource(path = folderName, contentType = contentType, options = sparkOptions)
      val result: DataFrame = fileDataSourceReader.read(mappingSourceBinding, mappingJobSourceSettings, Option.empty)

      // Validate the result
      result.count() shouldBe expectedRowNumber
      result.columns shouldBe expectedColumns
      result.collect().toSet should contain allElementsOf expectedRows
    }
  }

  /**
   * Tests that the FileDataSourceReader correctly reads data from JSON and NDJSON files.
   *
   * This test verifies that the reader can handle different content types and produce the expected results.
   * The test covers the following formats:
   * 1. JSON
   * 2. NDJSON (Text file with newline-delimited JSON content type)
   *
   * The test uses the following source binding configurations:
   * FileSystemSource(path = "patients.json", contentType = SourceContentTypes.JSON, options = Map("allowComments" -> "true"))
   * FileSystemSource(path = "patients-ndjson.txt", contentType = SourceContentTypes.NDJSON, options = Map("allowComments" -> "true"))
   *
   * The expected read result is for both content types:
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
  it should "correctly read from JSON and NDJSON files" in {
    // Folder including the test files
    val folderPath = "/single-file-test"

    // Define the expected values for validation (Note: Spark reads json columns in alphabetic order)
    val expectedRowNumber = 10
    val expectedColumns = Array("birthDate", "deceasedDateTime", "gender", "homePostalCode", "pid")
    val expectedFirstRow = Row("2000-05-10", null, "male", null, "p1")
    val expectedLastRow = Row("2003-11", null, "female", null, "p10")

    // Define the file names and their corresponding formats to be tested
    val sourceBindingConfigurations = Seq(
      ("patients.json", SourceContentTypes.JSON),
      ("patients-ndjson.txt", SourceContentTypes.NDJSON)
    )
    // Spark options to test if options are working
    val sparkOptions = Map(
      "allowComments" -> "true",
      "distinct" -> "true" // 'distinct' option randomly changes the order of the rows in the result.
    )

    // Loop through each source binding configuration to run the test
    val mappingJobSourceSettings = FileSystemSourceSettings(name = s"FileDataSourceReaderTest3", sourceUri = "test-uri", dataFolderPath = testDataFolderPath.concat(folderPath))
    sourceBindingConfigurations.foreach { case (fileName, contentType) =>
      // Define the source binding and settings for reading the file
      val mappingSourceBinding = FileSystemSource(path = fileName, contentType = contentType, options = sparkOptions)
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
   * This test verifies that the reader can handle multiple files across different content types
   * and produce the expected results. The test covers reading from folders containing:
   * 1. JSON (standard JSON files in the "json" folder)
   * 2. NDJSON (newline-delimited JSON files in the "ndjson" folder)
   *
   * The test uses the following source binding configurations:
   * FileSystemSource(path = "json", contentType = SourceContentTypes.JSON)
   * FileSystemSource(path = "ndjson", contentType = SourceContentTypes.NDJSON)
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

    // A sequence of folder names and content type of the files to be selected
    val sourceBindingConfigurations = Seq(
      ("json", SourceContentTypes.JSON),
      ("txt-ndjson", SourceContentTypes.NDJSON)
    )

    // Loop through each source binding configuration to run the test
    val mappingJobSourceSettings = FileSystemSourceSettings(name = "FileDataSourceReaderTest4", sourceUri = "test-uri", dataFolderPath = testDataFolderPath.concat(folderPath))
    sourceBindingConfigurations.foreach { case (folderName, contentType) =>
      // Read the data using the reader and the defined settings
      val mappingSourceBinding = FileSystemSource(path = folderName, contentType = contentType)
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
   * This test verifies that the reader can handle Parquet content type and produce the expected results.
   * The test covers the following format:
   * 1. PARQUET
   *
   * The test uses the following source binding configuration:
   * FileSystemSource(path = "patients.parquet", contentType = SourceContentTypes.PARQUET)
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

    // Define the file name and its corresponding content type for Parquet
    val sourceBindingConfigurations = Seq(
      ("patients.parquet", SourceContentTypes.PARQUET)
    )

    // Loop through each source binding configuration to run the test
    val mappingJobSourceSettings = FileSystemSourceSettings(name = s"FileDataSourceReaderTest5", sourceUri = "test-uri", dataFolderPath = testDataFolderPath.concat(folderPath))
    sourceBindingConfigurations.foreach { case (fileName, contentType) =>
      // Define the source binding and settings for reading the file
      val mappingSourceBinding = FileSystemSource(path = fileName, contentType = contentType)
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
   * This test verifies that the reader can handle Parquet content type and produce the expected results.
   * The test covers the following format:
   * 1. PARQUET
   *
   * The test uses the following source binding configuration:
   * FileSystemSource(path = "parquet", contentType = SourceContentTypes.PARQUET)
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

    // A sequence of folder names and content type of the files to be selected
    val sourceBindingConfigurations = Seq(
      ("parquet", SourceContentTypes.PARQUET)
    )

    // Loop through each source binding configuration to run the test
    val mappingJobSourceSettings = FileSystemSourceSettings(name = "FileDataSourceReaderTest6", sourceUri = "test-uri", dataFolderPath = testDataFolderPath.concat(folderPath))
    sourceBindingConfigurations.foreach { case (folderName, contentType) =>
      // Read the data using the reader and the defined settings
      val mappingSourceBinding = FileSystemSource(path = folderName, contentType = contentType)
      val result: DataFrame = fileDataSourceReader.read(mappingSourceBinding, mappingJobSourceSettings, Option.empty)

      // Validate the result
      result.count() shouldBe expectedRowNumber
      result.columns shouldBe expectedColumns
      result.collect().toSet should contain allElementsOf expectedRows
    }
  }

  /**
   * Tests that the FileDataSourceReader correctly reads multiple CSV, TSV, and TXT(csv) files from a zip file.
   *
   * This test verifies that the reader can handle multiple files inside a zip archive with different content types
   * and produce the expected results. The test covers reading from a zip file containing:
   * 1. CSV files
   * 2. TSV files
   * 3. TXT(csv) (Text files with CSV content type)
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
  it should "correctly read multiple CSV, TSV and TXT(csv) files from a zip archive" in {
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

    // A sequence of content types and their configuration for the test
    val sourceBindingConfigurations = Seq(
      ("csv.zip", SourceContentTypes.CSV),
      ("tsv.zip", SourceContentTypes.TSV),
      ("txt-csv.zip", SourceContentTypes.CSV)
    )

    // Spark options to test if options are working
    val sparkOptions = Map(
      "ignoreTrailingWhiteSpace" -> "true",
      "comment" -> "!"
    )

    // Test the reading from the zip file using the source configurations
    val mappingJobSourceSettings = FileSystemSourceSettings(name = "FileDataSourceReaderTest7", sourceUri = "zip-file-uri", dataFolderPath = testDataFolderPath.concat(folderPath))
    sourceBindingConfigurations.foreach { case (zipFileName, contentType) =>
      // Read the data using the reader and the defined settings
      val mappingSourceBinding = FileSystemSource(path = zipFileName, contentType = contentType, options = sparkOptions)
      val result: DataFrame = fileDataSourceReader.read(mappingSourceBinding, mappingJobSourceSettings, Option.empty)

      // Validate the result
      result.count() shouldBe expectedRowNumber
      result.columns shouldBe expectedColumns
      result.collect().toSet should contain allElementsOf expectedRows
    }
  }

  /**
   * Tests that the FileDataSourceReader correctly reads data from JSON and NDJSON files inside a zip archive.
   *
   * This test verifies that the reader can handle different content types (JSON, NDJSON) compressed in a zip file
   * and produce the expected results.
   *
   * The test covers the following formats inside a zip:
   * 1. JSON
   * 2. NDJSON (Text file with newline-delimited JSON format)
   *
   * The expected result for both content types:
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
  it should "correctly read from JSON and NDJSON files inside a zip archive" in {

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
      ("json.zip", SourceContentTypes.JSON), // JSON inside zip
      ("txt-ndjson.zip", SourceContentTypes.NDJSON) // Newline-delimited JSON inside zip
    )

    // Spark options for testing (e.g., allowing comments in the files)
    val sparkOptions = Map(
      "allowComments" -> "true"
    )

    // Loop through each zip file and perform the test
    val mappingJobSourceSettings = FileSystemSourceSettings(name = "FileDataSourceReaderTest8", sourceUri = "zip-uri", dataFolderPath = testDataFolderPath.concat(folderPath))

    sourceBindingConfigurations.foreach { case (zipFileName, contentType) =>

      // Define the source binding and read the data from the zip file
      val mappingSourceBinding = FileSystemSource(path = zipFileName, contentType = contentType, options = sparkOptions)
      val result: DataFrame = fileDataSourceReader.read(mappingSourceBinding, mappingJobSourceSettings, Option.empty)

      // Validate the result
      result.count() shouldBe expectedRowNumber
      result.columns shouldBe expectedColumns
      result.collect().toSet should contain allElementsOf expectedRows
    }
  }

}