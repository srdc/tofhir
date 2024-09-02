package io.tofhir.test.engine.data.write

import io.tofhir.engine.config.ToFhirConfig
import io.tofhir.engine.data.write.FileSystemWriter
import io.tofhir.engine.data.write.FileSystemWriter.SinkFileFormats
import io.tofhir.engine.model.{FhirMappingResult, FileSystemSinkSettings}
import io.tofhir.engine.util.FileUtils
import org.apache.spark.sql.delta.implicits.longEncoder
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.sql.Timestamp

/**
 * Unit tests for the FileSystemWriter class.
 * The tests validate the functionality of writing a DataFrame to a file system in different formats.
 */
class FileSystemWriterTest extends AnyFlatSpec with BeforeAndAfterAll {
  /**
   * SparkSession used for the test cases.
   */
  val sparkSession: SparkSession = ToFhirConfig.sparkSession
  /**
   * A DataFrame containing FHIR mapping results used as test data.
   */
  val df: Dataset[FhirMappingResult] = getTestDataFrame

  /**
   * Tests whether FileSystemWriter can write a DataFrame into an NDJSON file.
   *
   * FileSystemSinkSettings used for this test:
   * {
   *  "path": "output-ndjson",
   *  "fileFormat": "ndjson"
   * }
   *
   * The expected output structure is:
   *
   * > output-ndjson
   *  > part-00000-f746dffd-8346-41d6-9a8b-6c67231ea6bd-c000.txt.crc
   *  > part-00000-f746dffd-8346-41d6-9a8b-6c67231ea6bd-c000.txt
   * */
  it should "write DataFrame into a ndjson file" in {
    // Define the output path for the NDJSON files
    val outputFolderPath = s"${ToFhirConfig.engineConfig.contextPath}/output-ndjson"
    // Create a FileSystemWriter with NDJSON as the output format
    val fileSystemWriter = new FileSystemWriter(sinkSettings = FileSystemSinkSettings(
      path = outputFolderPath, fileFormat = Some(SinkFileFormats.NDJSON)
    ))
    // Write the DataFrame to the file system in NDJSON format
    fileSystemWriter.write(sparkSession, df, sparkSession.sparkContext.collectionAccumulator[FhirMappingResult])

    // Read the written NDJSON files back into a DataFrame
    val writtenDf = sparkSession.read
      .json(outputFolderPath)
    // Verify the total record count
    writtenDf.count() shouldBe 15
    // Group by resourceType and count
    val resultDf = writtenDf.groupBy("resourceType").count()
    // Check the count for "Patient" resourceType
    val patientCount = resultDf.filter(col("resourceType") === "Patient").select("count").as[Long].head()
    patientCount shouldBe 10
    // Check the count for "Condition" resourceType
    val conditionCount = resultDf.filter(col("resourceType") === "Condition").select("count").as[Long].head()
    conditionCount shouldBe 5
  }

  /**
   * Tests whether FileSystemWriter can write a DataFrame into an NDJSON file, partitioned by resource type.
   *
   * The test uses the following FileSystemSinkSettings:
   * {
   *  "path": "output-ndjson",
   *  "fileFormat": "ndjson"
   * }
   *
   * The expected output structure is:
   *
   * > output-ndjson-by-resource
   *  > Condition
   *    > .part-00000-5519e6da-a21c-45e9-b320-d0085e2901b4-c000.txt.crc
   *    > .part-00000-5519e6da-a21c-45e9-b320-d0085e2901b4-c000.txt
   *  > Patient
   *    > .part-00000-ba4e919a-88a0-4158-8d89-58aa45ef149f-c000.txt.crc
   *    > .part-00000-ba4e919a-88a0-4158-8d89-58aa45ef149f-c000.txt
   * */
  it should "write DataFrame as partitioned NDJSON files based on resource type" in {
    // Define the output path for the NDJSON files
    val outputFolderPath = s"${ToFhirConfig.engineConfig.contextPath}/output-ndjson-by-resource"
    // Instantiate the FileSystemWriter with NDJSON file format and resource type partitioning
    val fileSystemWriter = new FileSystemWriter(sinkSettings = FileSystemSinkSettings(
      path = outputFolderPath, fileFormat = Some(SinkFileFormats.NDJSON), partitionByResourceType = true
    ))
    // Write the DataFrame using the FileSystemWriter
    fileSystemWriter.write(sparkSession, df, sparkSession.sparkContext.collectionAccumulator[FhirMappingResult])

    // Verify that the data was correctly written and partitioned under "Condition"
    val conditionDf = sparkSession.read
      .json(s"$outputFolderPath/Condition")
    conditionDf.count() shouldBe 5
    // Verify that the data was correctly written and partitioned under "Patient"
    val patientDf = sparkSession.read
      .json(s"$outputFolderPath/Patient")
    patientDf.count() shouldBe 10
  }

  /**
   * Tests whether FileSystemWriter can write a DataFrame into a Parquet file.
   *
   * The test uses the following FileSystemSinkSettings:
   * {
   *  "path": "output-parquet",
   *  "fileFormat": "parquet"
   * }
   *
   * The expected output structure is:
   *
   * > output-parquet
   *  > part-00000-34382e7e-b916-4495-af23-d5714e921333-c000.snappy.parquet.crc
   *  > part-00000-34382e7e-b916-4495-af23-d5714e921333-c000.snappy.parquet
   * */
  it should "write DataFrame into a parquet file" in {
    // Define the output path for the parquet files
    val outputFolderPath = s"${ToFhirConfig.engineConfig.contextPath}/output-parquet"
    // Instantiate the FileSystemWriter with Parquet file format
    val fileSystemWriter = new FileSystemWriter(sinkSettings = FileSystemSinkSettings(
      path = outputFolderPath, fileFormat = Some(SinkFileFormats.PARQUET)
    ))
    // Write the DataFrame using the FileSystemWriter
    fileSystemWriter.write(sparkSession, df, sparkSession.sparkContext.collectionAccumulator[FhirMappingResult])

    // Read the written Parquet file back into a DataFrame
    val writtenDf = sparkSession.read
      .parquet(outputFolderPath)
    // Verify the total record count
    writtenDf.count() shouldBe 15
    // Group by resourceType and count
    val resultDf = writtenDf.groupBy("resourceType").count()
    // Check the count for "Patient" resourceType
    val patientCount = resultDf.filter(col("resourceType") === "Patient").select("count").as[Long].head()
    patientCount shouldBe 10
    // Check the count for "Condition" resourceType
    val conditionCount = resultDf.filter(col("resourceType") === "Condition").select("count").as[Long].head()
    conditionCount shouldBe 5
  }

  /**
   * Tests whether FileSystemWriter can write a DataFrame into a parquet file, partitioned by resource type.
   *
   * The test uses the following FileSystemSinkSettings:
   * {
   *  "path": "output-parquet-by-resource",
   *  "fileFormat": "parquet",
   *  "partitionByResourceType": true
   * }
   *
   * The expected output structure is:
   *
   * > output-parquet-by-resource
   *  > Condition
   *    > .part-00000-86f0fcc4-996b-4bb5-bba0-bae44724e988-c000.snappy.parquet.crc
   *    > .part-00000-86f0fcc4-996b-4bb5-bba0-bae44724e988-c000.snappy.parquet
   *  > Patient
   *    > .part-00000-ca276fd5-1c4f-4dba-8610-49e4b652a52d-c000.snappy.parquet.crc
   *    > .part-00000-ca276fd5-1c4f-4dba-8610-49e4b652a52d-c000.snappy.parquet
   * */
  it should "write DataFrame as partitioned parquet files based on resource type" in {
    // Define the output path for the parquet files
    val outputFolderPath = s"${ToFhirConfig.engineConfig.contextPath}/output-parquet-by-resource"
    // Instantiate the FileSystemWriter with parquet file format and resource type partitioning
    val fileSystemWriter = new FileSystemWriter(sinkSettings = FileSystemSinkSettings(
      path = outputFolderPath, fileFormat = Some(SinkFileFormats.PARQUET), partitionByResourceType = true
    ))
    // Write the DataFrame using the FileSystemWriter
    fileSystemWriter.write(sparkSession, df, sparkSession.sparkContext.collectionAccumulator[FhirMappingResult])

    // Verify that the data was correctly written and partitioned under "Condition"
    val conditionDf = sparkSession.read
      .parquet(s"$outputFolderPath/Condition")
    conditionDf.count() shouldBe 5
    // Verify that the data was correctly written and partitioned under "Patient"
    val patientDf = sparkSession.read
      .parquet(s"$outputFolderPath/Patient")
    patientDf.count() shouldBe 10
  }

  /**
   * Tests whether FileSystemWriter can write a DataFrame into a Delta Lake file.
   *
   * The test uses the following FileSystemSinkSettings:
   * {
   *  "path": "output-delta",
   *  "fileFormat": "delta"
   * }
   *
   * The expected output structure is:
   *
   * > output-delta
   *  > ._delta_log
   *    > .00000000000000000000.json.crc
   *    > 00000000000000000000.json
   *  > part-00000-98d3c5dd-3226-4359-8aea-f8b5c59e0fb3-c000.snappy.parquet.crc
   *  > part-00000-98d3c5dd-3226-4359-8aea-f8b5c59e0fb3-c000.snappy.parquet
   * */
  it should "write DataFrame into a Delta Lake file" in {
    // Define the output path for the Delta Lake files
    val outputFolderPath = s"${ToFhirConfig.engineConfig.contextPath}/output-delta"
    // Instantiate the FileSystemWriter with Delta Lake file format
    val fileSystemWriter = new FileSystemWriter(sinkSettings = FileSystemSinkSettings(
      path = outputFolderPath, fileFormat = Some(SinkFileFormats.DELTA_LAKE)
    ))
    // Write the DataFrame using the FileSystemWriter
    fileSystemWriter.write(sparkSession, df, sparkSession.sparkContext.collectionAccumulator[FhirMappingResult])

    // Read the written Delta Lake file back into a DataFrame
    val writtenDf = sparkSession.read
      .format(SinkFileFormats.DELTA_LAKE)
      .load(outputFolderPath)
    // Verify the total record count
    writtenDf.count() shouldBe 15
    // Group by resourceType and count
    val resultDf = writtenDf.groupBy("resourceType").count()
    // Check the count for "Patient" resourceType
    val patientCount = resultDf.filter(col("resourceType") === "Patient").select("count").as[Long].head()
    patientCount shouldBe 10
    // Check the count for "Condition" resourceType
    val conditionCount = resultDf.filter(col("resourceType") === "Condition").select("count").as[Long].head()
    conditionCount shouldBe 5
  }

  /**
   * Tests whether FileSystemWriter can write a DataFrame into a Delta Lake file, partitioned by resource type.
   *
   * The test uses the following FileSystemSinkSettings:
   * {
   *  "path": "output-delta-by-resource",
   *  "fileFormat": "delta",
   *  "partitionByResourceType": true
   * }
   *
   * The expected output structure is:
   *
   * > output-delta-by-resource
   *  > Condition
   *    > _delta_log
   *      > .00000000000000000000.json.crc
   *      > .00000000000000000000.json
   *    > .part-00000-fa92d07f-f5e3-4a65-8d68-6709aa2d0103-c000.snappy.parquet.crc
   *    > .part-00000-fa92d07f-f5e3-4a65-8d68-6709aa2d0103-c000.snappy.parquet
   *  > Patient
   *    > _delta_log
   *      > .00000000000000000000.json.crc
   *      > .00000000000000000000.json
   *    > .part-00000-ca276fd5-1c4f-4dba-8610-49e4b652a52d-c000.snappy.parquet.crc
   *    > .part-00000-ca276fd5-1c4f-4dba-8610-49e4b652a52d-c000.snappy.parquet
   * */
  it should "write DataFrame as partitioned Delta Lake files based on resource type" in {
    // Define the output path for the Delta Lake files
    val outputFolderPath = s"${ToFhirConfig.engineConfig.contextPath}/output-delta-by-resource"
    // Instantiate the FileSystemWriter with Delta Lake file format and resource type partitioning
    val fileSystemWriter = new FileSystemWriter(sinkSettings = FileSystemSinkSettings(
      path = outputFolderPath, fileFormat = Some(SinkFileFormats.DELTA_LAKE), partitionByResourceType = true
    ))
    // Write the DataFrame using the FileSystemWriter
    fileSystemWriter.write(sparkSession, df, sparkSession.sparkContext.collectionAccumulator[FhirMappingResult])

    // Verify that the data was correctly written and partitioned under "Condition"
    val conditionDf = sparkSession.read
      .format(SinkFileFormats.DELTA_LAKE)
      .load(s"$outputFolderPath/Condition")
    conditionDf.count() shouldBe 5
    // Verify that the data was correctly written and partitioned under "Patient"
    val patientDf = sparkSession.read
      .format(SinkFileFormats.DELTA_LAKE)
      .load(s"$outputFolderPath/Patient")
    patientDf.count() shouldBe 10
  }

  /**
   * Tests whether FileSystemWriter can write a DataFrame into a csv file.
   *
   * The test uses the following FileSystemSinkSettings:
   * {
   *  "path": "output-csv",
   *  "fileFormat": "csv",
   *  "options": {
   *    "header": true
   *  }
   * }
   *
   * The expected output structure is:
   *
   * > output-csv
   *  > .part-00000-755e8c9b-ac9b-4348-b81d-8dccfb6aeb56-c000.csv.crc
   *  > .part-00000-755e8c9b-ac9b-4348-b81d-8dccfb6aeb56-c000.csv
   * */
  it should "write DataFrame into a CSV file" in {
    // Define the output path for the csv files
    val outputFolderPath = s"${ToFhirConfig.engineConfig.contextPath}/output-csv"
    // Instantiate the FileSystemWriter with csv file format
    val fileSystemWriter = new FileSystemWriter(sinkSettings = FileSystemSinkSettings(
      path = outputFolderPath, fileFormat = Some(SinkFileFormats.CSV), options = Map("header" -> "true")
    ))
    // Write the DataFrame using the FileSystemWriter
    fileSystemWriter.write(sparkSession, df, sparkSession.sparkContext.collectionAccumulator[FhirMappingResult])

    // Read the written csv file back into a DataFrame
    val writtenDf = sparkSession.read
      .option("header", value = true)
      .csv(outputFolderPath)
    // Verify the total record count
    writtenDf.count() shouldBe 15
    // Since CSV is a flat file format, the DataFrame should only contain primitive fields, excluding any nested fields.
    writtenDf.columns.length shouldBe 8

    // Group by resourceType and count
    val resultDf = writtenDf.groupBy("resourceType").count()
    // Check the count for "Patient" resourceType
    val patientCount = resultDf.filter(col("resourceType") === "Patient").select("count").as[Long].head()
    patientCount shouldBe 10
    // Check the count for "Condition" resourceType
    val conditionCount = resultDf.filter(col("resourceType") === "Condition").select("count").as[Long].head()
    conditionCount shouldBe 5
  }

  /**
   * After the tests complete, delete the output folders.
   * */
  override protected def afterAll(): Unit = {
    super.afterAll()
    // delete context path
    org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath("").toFile)
  }

  /**
   * Helper method to create a test DataFrame containing FHIR mapping results. It includes 10 Patient and 5 Condition
   * FHIR Resources.
   *
   * @return A DataFrame with sample FHIR mapping results.
   */
  private def getTestDataFrame: Dataset[FhirMappingResult] = {
    import sparkSession.implicits._
    Seq(
      // Patients
      FhirMappingResult(
        jobId = "job",
        mappingUrl = "http://example.com/mapping",
        mappingExpr = Some("expression1"),
        timestamp = new Timestamp(System.currentTimeMillis()),
        mappedResource = Some("{\"resourceType\":\"Patient\",\"id\":\"34dc88d5972fd5472a942fc80f69f35c\",\"meta\":{\"profile\":[\"https://aiccelerate.eu/fhir/StructureDefinition/AIC-Patient\"],\"source\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\"},\"active\":true,\"identifier\":[{\"use\":\"official\",\"system\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\",\"value\":\"p1\"}],\"gender\":\"male\",\"birthDate\":\"2000-05-10\"}"),
        source = Some("Source"),
        error = None,
        fhirInteraction = None,
        executionId = Some("exec"),
        projectId = Some("project")
      ),
      FhirMappingResult(
        jobId = "job",
        mappingUrl = "http://example.com/mapping",
        mappingExpr = Some("expression"),
        timestamp = new Timestamp(System.currentTimeMillis()),
        mappedResource = Some("{\"resourceType\":\"Patient\",\"id\":\"0b3a0b23a0c6e223b941e63787f15a6a\",\"meta\":{\"profile\":[\"https://aiccelerate.eu/fhir/StructureDefinition/AIC-Patient\"],\"source\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\"},\"active\":true,\"identifier\":[{\"use\":\"official\",\"system\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\",\"value\":\"p2\"}],\"gender\":\"male\",\"birthDate\":\"1985-05-08\",\"deceasedDateTime\":\"2017-03-10\",\"address\":[{\"use\":\"home\",\"type\":\"both\",\"postalCode\":\"G02547\"}]}"),
        source = Some("Source"),
        error = None,
        fhirInteraction = None,
        executionId = Some("exec"),
        projectId = Some("project")
      ),
      FhirMappingResult(
        jobId = "job",
        mappingUrl = "http://example.com/mapping",
        mappingExpr = Some("expression"),
        timestamp = new Timestamp(System.currentTimeMillis()),
        mappedResource = Some("{\"resourceType\":\"Patient\",\"id\":\"49d3c335681ab7fb2d4cdf19769655db\",\"meta\":{\"profile\":[\"https://aiccelerate.eu/fhir/StructureDefinition/AIC-Patient\"],\"source\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\"},\"active\":true,\"identifier\":[{\"use\":\"official\",\"system\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\",\"value\":\"p3\"}],\"gender\":\"male\",\"birthDate\":\"1997-02\"}"),
        source = Some("Source"),
        error = None,
        fhirInteraction = None,
        executionId = Some("exec"),
        projectId = Some("project")
      ),
      FhirMappingResult(
        jobId = "job",
        mappingUrl = "http://example.com/mapping",
        mappingExpr = Some("expression"),
        timestamp = new Timestamp(System.currentTimeMillis()),
        mappedResource = Some("{\"resourceType\":\"Patient\",\"id\":\"0bbad2343eb86d5cdc16a1b292537576\",\"meta\":{\"profile\":[\"https://aiccelerate.eu/fhir/StructureDefinition/AIC-Patient\"],\"source\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\"},\"active\":true,\"identifier\":[{\"use\":\"official\",\"system\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\",\"value\":\"p4\"}],\"gender\":\"male\",\"birthDate\":\"1999-06-05\",\"address\":[{\"use\":\"home\",\"type\":\"both\",\"postalCode\":\"H10564\"}]}"),
        source = Some("Source"),
        error = None,
        fhirInteraction = None,
        executionId = Some("exec"),
        projectId = Some("project")
      ),
      FhirMappingResult(
        jobId = "job",
        mappingUrl = "http://example.com/mapping",
        mappingExpr = Some("expression"),
        timestamp = new Timestamp(System.currentTimeMillis()),
        mappedResource = Some("{\"resourceType\":\"Patient\",\"id\":\"7b650be0176d6d29351f84314a5efbe3\",\"meta\":{\"profile\":[\"https://aiccelerate.eu/fhir/StructureDefinition/AIC-Patient\"],\"source\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\"},\"active\":true,\"identifier\":[{\"use\":\"official\",\"system\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\",\"value\":\"p5\"}],\"gender\":\"male\",\"birthDate\":\"1965-10-01\",\"deceasedDateTime\":\"2019-04-21\",\"address\":[{\"use\":\"home\",\"type\":\"both\",\"postalCode\":\"G02547\"}]}"),
        source = Some("Source"),
        error = None,
        fhirInteraction = None,
        executionId = Some("exec"),
        projectId = Some("project")
      ),
      FhirMappingResult(
        jobId = "job",
        mappingUrl = "http://example.com/mapping",
        mappingExpr = Some("expression"),
        timestamp = new Timestamp(System.currentTimeMillis()),
        mappedResource = Some("{\"resourceType\":\"Patient\",\"id\":\"17c7c9664ac82f384de0ad4625f2ae4c\",\"meta\":{\"profile\":[\"https://aiccelerate.eu/fhir/StructureDefinition/AIC-Patient\"],\"source\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\"},\"active\":true,\"identifier\":[{\"use\":\"official\",\"system\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\",\"value\":\"p6\"}],\"gender\":\"female\",\"birthDate\":\"1991-03\"}"),
        source = Some("Source"),
        error = None,
        fhirInteraction = None,
        executionId = Some("exec"),
        projectId = Some("project")
      ),
      FhirMappingResult(
        jobId = "job",
        mappingUrl = "http://example.com/mapping",
        mappingExpr = Some("expression"),
        timestamp = new Timestamp(System.currentTimeMillis()),
        mappedResource = Some("{\"resourceType\":\"Patient\",\"id\":\"e1ea114dcfcea572982f224e43deb7a6\",\"meta\":{\"profile\":[\"https://aiccelerate.eu/fhir/StructureDefinition/AIC-Patient\"],\"source\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\"},\"active\":true,\"identifier\":[{\"use\":\"official\",\"system\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\",\"value\":\"p7\"}],\"gender\":\"female\",\"birthDate\":\"1972-10-25\",\"address\":[{\"use\":\"home\",\"type\":\"both\",\"postalCode\":\"V13135\"}]}"),
        source = Some("Source"),
        error = None,
        fhirInteraction = None,
        executionId = Some("exec"),
        projectId = Some("project")
      ),
      FhirMappingResult(
        jobId = "job",
        mappingUrl = "http://example.com/mapping",
        mappingExpr = Some("expression"),
        timestamp = new Timestamp(System.currentTimeMillis()),
        mappedResource = Some("{\"resourceType\":\"Patient\",\"id\":\"f6bf84d63799f65dcdd4f5027021adf3\",\"meta\":{\"profile\":[\"https://aiccelerate.eu/fhir/StructureDefinition/AIC-Patient\"],\"source\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\"},\"active\":true,\"identifier\":[{\"use\":\"official\",\"system\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\",\"value\":\"p8\"}],\"gender\":\"female\",\"birthDate\":\"2010-01-10\",\"address\":[{\"use\":\"home\",\"type\":\"both\",\"postalCode\":\"Z54564\"}]}"),
        source = Some("Source"),
        error = None,
        fhirInteraction = None,
        executionId = Some("exec"),
        projectId = Some("project")
      ),
      FhirMappingResult(
        jobId = "job",
        mappingUrl = "http://example.com/mapping",
        mappingExpr = Some("expression"),
        timestamp = new Timestamp(System.currentTimeMillis()),
        mappedResource = Some("{\"resourceType\":\"Patient\",\"id\":\"a06f7d449f8a655d9163204f0de8237f\",\"meta\":{\"profile\":[\"https://aiccelerate.eu/fhir/StructureDefinition/AIC-Patient\"],\"source\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\"},\"active\":true,\"identifier\":[{\"use\":\"official\",\"system\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\",\"value\":\"p9\"}],\"gender\":\"female\",\"birthDate\":\"1999-05-12\"}"),
        source = Some("Source"),
        error = None,
        fhirInteraction = None,
        executionId = Some("exec"),
        projectId = Some("project")
      ),
      FhirMappingResult(
        jobId = "job",
        mappingUrl = "http://example.com/mapping",
        mappingExpr = Some("expression"),
        timestamp = new Timestamp(System.currentTimeMillis()),
        mappedResource = Some("{\"resourceType\":\"Patient\",\"id\":\"7bd4fad75b1efbdc50859a736b839e24\",\"meta\":{\"profile\":[\"https://aiccelerate.eu/fhir/StructureDefinition/AIC-Patient\"],\"source\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\"},\"active\":true,\"identifier\":[{\"use\":\"official\",\"system\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\",\"value\":\"p10\"}],\"gender\":\"female\",\"birthDate\":\"2003-11\"}"),
        source = Some("Source"),
        error = None,
        fhirInteraction = None,
        executionId = Some("exec"),
        projectId = Some("project")
      ),
      // Conditions
      FhirMappingResult(
        jobId = "job",
        mappingUrl = "http://example.com/mapping",
        mappingExpr = Some("expression"),
        timestamp = new Timestamp(System.currentTimeMillis()),
        mappedResource = Some("{\"resourceType\":\"Condition\",\"id\":\"2faab6373e7c3bba4c1971d089fc6407\",\"meta\":{\"profile\":[\"https://aiccelerate.eu/fhir/StructureDefinition/AIC-Condition\"],\"source\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\"},\"clinicalStatus\":{\"coding\":[{\"system\":\"http://terminology.hl7.org/CodeSystem/condition-clinical\",\"code\":\"active\"}]},\"verificationStatus\":{\"coding\":[{\"system\":\"http://terminology.hl7.org/CodeSystem/condition-ver-status\",\"code\":\"confirmed\"}]},\"category\":[{\"coding\":[{\"system\":\"http://terminology.hl7.org/CodeSystem/condition-category\",\"code\":\"problem-list-item\"}]}],\"code\":{\"coding\":[{\"system\":\"http://hl7.org/fhir/sid/icd-10\",\"code\":\"J13\",\"display\":\"Pneumonia due to Streptococcus pneumoniae\"}]},\"subject\":{\"reference\":\"Patient/34dc88d5972fd5472a942fc80f69f35c\"},\"onsetDateTime\":\"2012-10-15\"}"),
        source = Some("Source"),
        error = None,
        fhirInteraction = None,
        executionId = Some("exec"),
        projectId = Some("project")
      ),
      FhirMappingResult(
        jobId = "job",
        mappingUrl = "http://example.com/mapping",
        mappingExpr = Some("expression"),
        timestamp = new Timestamp(System.currentTimeMillis()),
        mappedResource = Some("{\"resourceType\":\"Condition\",\"id\":\"63058b87a718e66d4198703675b0204a\",\"meta\":{\"profile\":[\"https://aiccelerate.eu/fhir/StructureDefinition/AIC-Condition\"],\"source\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\"},\"clinicalStatus\":{\"coding\":[{\"system\":\"http://terminology.hl7.org/CodeSystem/condition-clinical\",\"code\":\"inactive\"}]},\"category\":[{\"coding\":[{\"system\":\"http://terminology.hl7.org/CodeSystem/condition-category\",\"code\":\"encounter-diagnosis\"}]}],\"code\":{\"coding\":[{\"system\":\"http://hl7.org/fhir/sid/icd-10\",\"code\":\"G40\",\"display\":\"Parkinson\"}]},\"subject\":{\"reference\":\"Patient/0b3a0b23a0c6e223b941e63787f15a6a\"},\"encounter\":{\"reference\":\"Encounter/bb7134de6cdbf64352b074e9d2555adb\"},\"onsetDateTime\":\"2013-05-07\",\"abatementDateTime\":\"2013-05-22\"}"),
        source = Some("Source"),
        error = None,
        fhirInteraction = None,
        executionId = Some("exec"),
        projectId = Some("project")
      ),
      FhirMappingResult(
        jobId = "job",
        mappingUrl = "http://example.com/mapping",
        mappingExpr = Some("expression"),
        timestamp = new Timestamp(System.currentTimeMillis()),
        mappedResource = Some("{\"resourceType\":\"Condition\",\"id\":\"ec4aed2cb844c70104e467fad58f6a44\",\"meta\":{\"profile\":[\"https://aiccelerate.eu/fhir/StructureDefinition/AIC-Condition\"],\"source\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\"},\"clinicalStatus\":{\"coding\":[{\"system\":\"http://terminology.hl7.org/CodeSystem/condition-clinical\",\"code\":\"inactive\"}]},\"verificationStatus\":{\"coding\":[{\"system\":\"http://terminology.hl7.org/CodeSystem/condition-ver-status\",\"code\":\"unconfirmed\"}]},\"category\":[{\"coding\":[{\"system\":\"http://terminology.hl7.org/CodeSystem/condition-category\",\"code\":\"encounter-diagnosis\"}]}],\"code\":{\"coding\":[{\"system\":\"http://hl7.org/fhir/sid/icd-10\",\"code\":\"J85\",\"display\":\"Abscess of lung and mediastinum\"}]},\"subject\":{\"reference\":\"Patient/49d3c335681ab7fb2d4cdf19769655db\"},\"onsetDateTime\":\"2016-02-11\",\"asserter\":{\"reference\":\"Practitioner/09361569c5dee906d244968c680cf2b4\"}}"),
        source = Some("Source"),
        error = None,
        fhirInteraction = None,
        executionId = Some("exec"),
        projectId = Some("project")
      ),
      FhirMappingResult(
        jobId = "job",
        mappingUrl = "http://example.com/mapping",
        mappingExpr = Some("expression"),
        timestamp = new Timestamp(System.currentTimeMillis()),
        mappedResource = Some("{\"resourceType\":\"Condition\",\"id\":\"6e0337f749b68a5450efb3fe6f918362\",\"meta\":{\"profile\":[\"https://aiccelerate.eu/fhir/StructureDefinition/AIC-Condition\"],\"source\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\"},\"clinicalStatus\":{\"coding\":[{\"system\":\"http://terminology.hl7.org/CodeSystem/condition-clinical\",\"code\":\"inactive\"}]},\"category\":[{\"coding\":[{\"system\":\"http://terminology.hl7.org/CodeSystem/condition-category\",\"code\":\"encounter-diagnosis\"}]}],\"code\":{\"coding\":[{\"system\":\"http://hl7.org/fhir/sid/icd-10\",\"code\":\"M89.9\",\"display\":\"Disorder of bone, unspecified\"}]},\"subject\":{\"reference\":\"Patient/0bbad2343eb86d5cdc16a1b292537576\"},\"onsetDateTime\":\"2014-01-05T10:00:00Z\"}"),
        source = Some("Source"),
        error = None,
        fhirInteraction = None,
        executionId = Some("exec"),
        projectId = Some("project")
      ),
      FhirMappingResult(
        jobId = "job",
        mappingUrl = "http://example.com/mapping",
        mappingExpr = Some("expression"),
        timestamp = new Timestamp(System.currentTimeMillis()),
        mappedResource = Some("{\"resourceType\":\"Condition\",\"id\":\"14ce4f8a1b8161ad59e1a8d67ce8d06d\",\"meta\":{\"profile\":[\"https://aiccelerate.eu/fhir/StructureDefinition/AIC-Condition\"],\"source\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\"},\"clinicalStatus\":{\"coding\":[{\"system\":\"http://terminology.hl7.org/CodeSystem/condition-clinical\",\"code\":\"inactive\"}]},\"category\":[{\"coding\":[{\"system\":\"http://terminology.hl7.org/CodeSystem/condition-category\",\"code\":\"encounter-diagnosis\"}]}],\"code\":{\"coding\":[{\"system\":\"http://hl7.org/fhir/sid/icd-10\",\"code\":\"G40.419\",\"display\":\"Other generalized epilepsy and epileptic syndromes, intractable, without status epilepticus\"}]},\"subject\":{\"reference\":\"Patient/7b650be0176d6d29351f84314a5efbe3\"},\"onsetDateTime\":\"2009-04-07\",\"asserter\":{\"reference\":\"Practitioner/b2e43c8d7dae698f539b1924679a7814\"}}"),
        source = Some("Source"),
        error = None,
        fhirInteraction = None,
        executionId = Some("exec"),
        projectId = Some("project")
      ),
    ).toDS()
  }
}
