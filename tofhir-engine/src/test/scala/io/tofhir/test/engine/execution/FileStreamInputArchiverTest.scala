package io.tofhir.test.engine.execution

import io.tofhir.engine.config.ToFhirConfig
import io.tofhir.engine.execution.{FileStreamInputArchiver, RunningJobRegistry}
import io.tofhir.engine.model._
import io.tofhir.engine.util.FileUtils
import org.mockito.MockitoSugar._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{File, PrintWriter}
import scala.reflect.runtime.universe._

class FileStreamInputArchiverTest extends AnyFlatSpec with Matchers {

  val runningJobRegistryMock: RunningJobRegistry = mock[RunningJobRegistry]
  val fileStreamInputArchiver = new FileStreamInputArchiver(runningJobRegistryMock)

  // Create test objects for archive mode
  val mappingUrl = "mocked_mapping_url"
  val jobId = "mocked_job_id"
  val sourceFolderPath = "test-archiver-batch"
  val inputFilePath = "test-input-file"
  val testSourceSettings: FileSystemSourceSettings = FileSystemSourceSettings(name = "test", sourceUri = "test", dataFolderPath = sourceFolderPath)
  val testFileSystemSource: FileSystemSource = FileSystemSource(path = inputFilePath)
  val testMappingTask: FhirMappingTask = FhirMappingTask(sourceContext = Map("_" -> testFileSystemSource), mappingRef = "test")
  val testSinkSettings: FhirRepositorySinkSettings = FhirRepositorySinkSettings(fhirRepoUrl = "test")
  val testDataProcessingSettings: DataProcessingSettings = DataProcessingSettings(archiveMode = ArchiveModes.ARCHIVE)
  val testJob: FhirMappingJob = FhirMappingJob(id = jobId, dataProcessingSettings = testDataProcessingSettings,
    sourceSettings = Map(("_") -> testSourceSettings), sinkSettings = testSinkSettings, mappings = Seq.empty)
  val testExecution: FhirMappingJobExecution = FhirMappingJobExecution(id = jobId, job = testJob, mappingTasks = Seq(testMappingTask))

  // Create test objects for delete mode
  val jobId2 = "mocked_job_id_2"
  val testDataProcessingSettingsWithDelete: DataProcessingSettings = DataProcessingSettings(archiveMode = ArchiveModes.DELETE)
  val testJobWithDelete: FhirMappingJob = testJob.copy(id = jobId2, dataProcessingSettings = testDataProcessingSettingsWithDelete)
  val testExecutionWithDelete: FhirMappingJobExecution = testExecution.copy(id = jobId2, job = testJobWithDelete)

  "FileStreamInputArchiver" should "apply archiving for a streaming job" in {

    // Initialize spark files for this test
    val testCsvFile: File = initializeSparkFiles(jobId, mappingUrl)

    // Find the relative path between the workspace folder and the file to be archived
    val relPath = FileUtils.getPath("").toAbsolutePath.relativize(FileUtils.getPath(ToFhirConfig.sparkCheckpointDirectory, "test.csv").toAbsolutePath)
    val expectedRelPath = FileUtils.getPath(ToFhirConfig.sparkCheckpointDirectory, "test.csv")
    // validate relPath
    relPath shouldBe expectedRelPath
    // The relative path is appended to the base archive folder so that the path of the original input file is preserved
    val finalArchivePath = FileUtils.getPath(ToFhirConfig.engineConfig.archiveFolder, relPath.toString)

    // Check whether archiving file does not exist and csv file exists
    finalArchivePath.toFile.exists() shouldBe false
    testCsvFile.exists() shouldBe true

    // Call archiving function
    fileStreamInputArchiver.applyArchivingOnStreamingJob(testExecution, mappingUrl)

    // Check whether archiving file exists and csv file is moved
    finalArchivePath.toFile.exists() shouldBe true
    testCsvFile.exists() shouldBe false

    // Clean test directory
    org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(ToFhirConfig.sparkCheckpointDirectory).toFile)
    org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(ToFhirConfig.engineConfig.archiveFolder).toFile)
  }

  "FileStreamInputArchiver" should "apply deletion for a streaming job" in {

    // Initialize spark files for this test
    val testCsvFile: File = initializeSparkFiles(jobId2, mappingUrl)

    // Check whether csv file exists
    testCsvFile.exists() shouldBe true

    // Call archiving function
    fileStreamInputArchiver.applyArchivingOnStreamingJob(testExecutionWithDelete, mappingUrl)

    // Check whether csv file is deleted
    testCsvFile.exists() shouldBe false

    // Clean test directory
    org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(ToFhirConfig.sparkCheckpointDirectory).toFile)
  }

  "FileStreamInputArchiver" should "apply archiving for a batch job" in{

    // Create a test input file
    val inputFile = initializeInputFiles(sourceFolderPath, inputFilePath)

    // Find the relative path between the workspace folder and the file to be archived
    val relPath = FileUtils.getPath("").toAbsolutePath.relativize(inputFile.toPath.toAbsolutePath)
    val expectedRelPath = inputFile.toPath
    // validate relPath
    relPath shouldBe expectedRelPath
    // The relative path is appended to the base archive folder so that the path of the original input file is preserved
    val finalArchivePath = FileUtils.getPath(ToFhirConfig.engineConfig.archiveFolder, relPath.toString)

    // Check whether archiving file does not exist and input file exists
    finalArchivePath.toFile.exists() shouldBe false
    inputFile.exists() shouldBe true

    //Call archiving function
    FileStreamInputArchiver.applyArchivingOnBatchJob(testExecution)

    // Check whether archiving file exists and input file is moved
    finalArchivePath.toFile.exists() shouldBe true
    inputFile.exists() shouldBe false

    // Clean test directories
    org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(ToFhirConfig.engineConfig.contextPath, sourceFolderPath).toFile)
    org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(ToFhirConfig.engineConfig.archiveFolder).toFile)
  }

  "FileStreamInputArchiver" should "apply deletion for a batch job" in{

    // Create a test input file
    val inputFile = initializeInputFiles(sourceFolderPath, inputFilePath)

    // Check whether input file exists
    inputFile.exists() shouldBe true

    //Call archiving function
    FileStreamInputArchiver.applyArchivingOnBatchJob(testExecutionWithDelete)

    // Check whether input file is deleted
    inputFile.exists() shouldBe false

    // Clean test directories
    org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(ToFhirConfig.engineConfig.contextPath, sourceFolderPath).toFile)
  }

  "FileStreamInputArchiver" should "get input files" in {

    val mappingUrl = "mocked_mapping_url"
    val jobId = "mocked_job_id_3"

    // Create a source file to refer location of test.csv
    val sourceFile = FileUtils.getPath("test-archiver", jobId, mappingUrl.hashCode.toString, "sources", "0", "0").toFile
    // Path of test.csv and test2.csv
    val testCsvFile = FileUtils.getPath("test-archiver", "test.csv").toFile
    val test2CsvFile = FileUtils.getPath("test-archiver", "test2.csv").toFile
    // Ensure the parent directories exist, if not, create them
    sourceFile.getParentFile.mkdirs()
    testCsvFile.getParentFile.mkdirs()
    test2CsvFile.getParentFile.mkdirs()
    // Write path of test.csv and test2.csv to the source file
    val sourceWriter = new PrintWriter(sourceFile)
    sourceWriter.write(s"{\"path\":\"${testCsvFile.getAbsolutePath.replace("\\", "\\\\")}\"}\n")
    sourceWriter.write(s"{\"path\":\"${test2CsvFile.getAbsolutePath.replace("\\", "\\\\")}\"}\n")
    sourceWriter.close()

    // Access getInputFile method of FileStreamInputArchiver using reflection
    val FileStreamInputArchiverInstance = FileStreamInputArchiver
    val methodSymbol = typeOf[FileStreamInputArchiver.type].decl(TermName("getInputFiles")).asMethod
    val methodMirror = runtimeMirror(getClass.getClassLoader).reflect(FileStreamInputArchiverInstance)
    val getInputFilesMethod = methodMirror.reflectMethod(methodSymbol)

    // Call reflected getInputFile function
    val result: Seq[File] = getInputFilesMethod(sourceFile).asInstanceOf[Seq[File]]

    // Check whether result is as expected
    result.size shouldBe 2
    result.head.getName shouldBe "test.csv"
    result.last.getName shouldBe "test2.csv"

    // Clean test directory
    org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath("test-archiver").toFile)
  }

  "FileStreamInputArchiver" should "get last commit offset" in {

    val mappingUrl = "mocked_mapping_url"
    val jobId = "mocked_job_id_4"

    // Create a source file to refer location of test.csv
    val commitFile = FileUtils.getPath("test-archiver", jobId, mappingUrl.hashCode.toString, "commit", "0", "0").toFile
    val commitFile2 = FileUtils.getPath("test-archiver", jobId, mappingUrl.hashCode.toString, "commit", "0", "1").toFile
    val commitDirectory = FileUtils.getPath("test-archiver", jobId, mappingUrl.hashCode.toString, "commit", "0").toFile
    // Ensure the parent directories exist, if not, create them
    commitFile.getParentFile.mkdirs()
    commitFile2.getParentFile.mkdirs()

    // Create commit files
    val commitWriter = new PrintWriter(commitFile)
    commitWriter.write("test")
    commitWriter.close()
    val commitWriter2 = new PrintWriter(commitFile2)
    commitWriter2.write("test")
    commitWriter2.close()

    // Access getLastCommitOffset method of FileStreamInputArchiver using reflection
    val FileStreamInputArchiverInstance = FileStreamInputArchiver
    val methodSymbol = typeOf[FileStreamInputArchiver.type].decl(TermName("getLastCommitOffset")).asMethod
    val methodMirror = runtimeMirror(getClass.getClassLoader).reflect(FileStreamInputArchiverInstance)
    val getLastCommitOffsetMethod = methodMirror.reflectMethod(methodSymbol)

    // Call reflected getLastCommitOffset function
    val result: Int = getLastCommitOffsetMethod(commitDirectory).asInstanceOf[Int]
    // Check whether result is as expected
    result shouldBe 1

    // Clean test directory
    org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath("test-archiver").toFile)
  }

  "FileStreamInputArchiver" should "get last proccessed offset" in {

    val mappingUrl = "mocked_mapping_url"
    val jobId = "mocked_job_id_5"

    // Get map of processed offset
    val processedOffsetMap = fileStreamInputArchiver.processedOffsets

    // Access getOffsetKey method of FileStreamInputArchiver using reflection
    val FileStreamInputArchiverInstance = FileStreamInputArchiver
    val methodSymbol = typeOf[FileStreamInputArchiver.type].decl(TermName("getOffsetKey")).asMethod
    val methodMirror = runtimeMirror(getClass.getClassLoader).reflect(FileStreamInputArchiverInstance)
    val getOffsetKeyMethod = methodMirror.reflectMethod(methodSymbol)

    // Call reflected getOffsetKey function
    val  lastProccessedOffset = processedOffsetMap.getOrElseUpdate(getOffsetKeyMethod(jobId, mappingUrl).asInstanceOf[String], -1)
    // Check whether lastProccessedOffset is as expected
    lastProccessedOffset shouldBe -1

    // Update lastProccessedOffset
    processedOffsetMap.put(getOffsetKeyMethod(jobId, mappingUrl).asInstanceOf[String], 2)

    // Call reflected getOffsetKey function
    val newLastProccessedOffset = processedOffsetMap.getOrElseUpdate(getOffsetKeyMethod(jobId, mappingUrl).asInstanceOf[String], -1)

    // Check whether lastProccessedOffset is updated
    newLastProccessedOffset shouldBe 2
  }

  /**
   * Initialize needed spark files.
   * @param jobId Job id of the execution.
   * @param mappingUrl Selected mapping url.
   * @return Return test csv file
   */
  private def initializeSparkFiles(jobId: String, mappingUrl: String): File = {
    // Create a source file to refer location of test.csv
    val sourceFile = FileUtils.getPath(ToFhirConfig.sparkCheckpointDirectory, jobId, mappingUrl.hashCode.toString, "sources", "0", "0").toFile
    // Path of test.csv
    val testCsvFile = FileUtils.getPath(ToFhirConfig.sparkCheckpointDirectory, "test.csv").toFile
    // Ensure the parent directories exist, if not, create them
    sourceFile.getParentFile.mkdirs()
    testCsvFile.getParentFile.mkdirs()
    // Write path of test.csv to the source file
    val sourceWriter = new PrintWriter(sourceFile)
    sourceWriter.write(s"{\"path\":\"${testCsvFile.getAbsolutePath.replace("\\", "\\\\")}\"}")
    sourceWriter.close()

    // Create a test csv
    val testCsvWriter = new PrintWriter(testCsvFile)
    testCsvWriter.write("testColumn1,testColumn2,testColumn3,testColumn4,testColumn5\ntestRow1,testRow2,testRow3,testRow4,testRow5")
    testCsvWriter.close()

    // Create a commit file inorder to start range function in applyArchivingOnStreamingJob
    val commitFile = FileUtils.getPath(ToFhirConfig.sparkCheckpointDirectory, jobId, mappingUrl.hashCode.toString, "commits", "0").toFile
    // Ensure the parent directories exist, if not, create them
    commitFile.getParentFile.mkdirs()
    val commitWriter = new PrintWriter(commitFile)
    commitWriter.write("v1\n{\"nextBatchWatermarkMs\":0}")
    commitWriter.close()

    testCsvFile
  }

  /**
   * Initialize needed input files.
   * @param sourceFolderPath Source folder path for input file
   * @param inputFileName Input file name
   * @return Return input file
   */
  private def initializeInputFiles(sourceFolderPath: String, inputFileName: String): File = {
    // Create a test input file
    val inputFile = FileUtils.getPath(ToFhirConfig.engineConfig.contextPath, sourceFolderPath, inputFileName).toFile
    inputFile.getParentFile.mkdirs()
    // Write test to input file
    val inputWriter = new PrintWriter(inputFile)
    inputWriter.write("test")
    inputWriter.close()

    inputFile
  }
}
