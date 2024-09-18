package io.tofhir.test.engine.execution

import io.tofhir.engine.config.ToFhirConfig
import io.tofhir.engine.execution.{RunningJobRegistry}
import io.tofhir.engine.execution.processing.FileStreamInputArchiver
import io.tofhir.engine.model._
import io.tofhir.engine.util.{FileUtils, SparkUtil}
import org.mockito.MockitoSugar._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{File, PrintWriter}
import java.nio.file.Paths
import scala.reflect.runtime.universe._

class FileStreamInputArchiverTest extends AnyFlatSpec with Matchers {

  val runningJobRegistryMock: RunningJobRegistry = mock[RunningJobRegistry]
  val fileStreamInputArchiver = new FileStreamInputArchiver(runningJobRegistryMock)

  // Create test objects for archive mode
  val mappingTaskName = "mocked_mappingTask_name"
  val jobId = "mocked_job_id"
  val sourceFolderPath = "test-archiver-batch"
  val inputFilePath = "test-input-file"
  val testSourceSettings: FileSystemSourceSettings = FileSystemSourceSettings(name = "test", sourceUri = "test", dataFolderPath = sourceFolderPath)
  val testFileSystemSource: FileSystemSource = FileSystemSource(path = inputFilePath, contentType = SourceContentTypes.CSV)
  val testMappingTask: FhirMappingTask = FhirMappingTask(name = "test", sourceBinding = Map("_" -> testFileSystemSource), mappingRef = "test")
  val testSinkSettings: FhirRepositorySinkSettings = FhirRepositorySinkSettings(fhirRepoUrl = "test")
  val testDataProcessingSettings: DataProcessingSettings = DataProcessingSettings(archiveMode = ArchiveModes.ARCHIVE)
  val testJob: FhirMappingJob = FhirMappingJob(id = jobId, dataProcessingSettings = testDataProcessingSettings,
    sourceSettings = Map(("_") -> testSourceSettings), sinkSettings = testSinkSettings, mappings = Seq.empty)
  val testExecution: FhirMappingJobExecution = FhirMappingJobExecution(id = jobId, job = testJob, mappingTasks = Seq(testMappingTask))

  // Create test objects for delete mode
  val jobId2 = "mocked_job_id_2"
  val testExecutionWithDelete: FhirMappingJobExecution = testExecution.copy(id = jobId2, jobId = jobId2, archiveMode = ArchiveModes.DELETE)

  // Create test objects for off mode
  val jobId3 = "mocked_job_id_3"
  val testExecutionWithOff: FhirMappingJobExecution = testExecution.copy(id = jobId3, jobId = jobId3 , archiveMode = ArchiveModes.OFF)

  "FileStreamInputArchiver" should "not apply archiving/deletion for a streaming job with archive mode is off" in {
    // Initialize spark files for this test
    val testCsvFile: File = initializeSparkFiles(jobId3, mappingTaskName)

    // Check whether csv file exists
    testCsvFile.exists() shouldBe true

    // Call archiving function
    fileStreamInputArchiver.applyArchivingOnStreamingJob(testExecutionWithOff, mappingTaskName)

    // Check whether csv file remains
    testCsvFile.exists() shouldBe true

    // Clean test directory
    org.apache.commons.io.FileUtils.deleteDirectory(new File(ToFhirConfig.sparkCheckpointDirectory))
  }

  "FileStreamInputArchiver" should "apply archiving for a streaming job" in {

    // Initialize spark files for this test
    val testCsvFile: File = initializeSparkFiles(jobId, mappingTaskName)

    // Find the relative path between the workspace folder and the file to be archived
    val relPath = FileUtils.getPath("").toAbsolutePath.relativize(Paths.get(ToFhirConfig.sparkCheckpointDirectory, "test.csv").toAbsolutePath)
    // The relative path is appended to the base archive folder so that the path of the original input file is preserved
    val finalArchivePath = Paths.get(ToFhirConfig.engineConfig.archiveFolder, relPath.toString)

    // Check whether archiving file does not exist and csv file exists
    finalArchivePath.toFile.exists() shouldBe false
    testCsvFile.exists() shouldBe true

    // Call archiving function
    fileStreamInputArchiver.applyArchivingOnStreamingJob(testExecution, mappingTaskName)

    // Check whether archiving file exists and csv file is moved
    finalArchivePath.toFile.exists() shouldBe true
    testCsvFile.exists() shouldBe false

    // Apply archiving using a new FileStreamInputArchiver to simulate the rerun of a streaming job
    new FileStreamInputArchiver(runningJobRegistryMock).applyArchivingOnStreamingJob(testExecution, mappingTaskName)
    // Check whether archiving file still exists
    finalArchivePath.toFile.exists() shouldBe true
    // Clean test directory
    org.apache.commons.io.FileUtils.deleteDirectory(new File(ToFhirConfig.sparkCheckpointDirectory))
    org.apache.commons.io.FileUtils.deleteDirectory(new File(ToFhirConfig.engineConfig.archiveFolder))
  }

  "FileStreamInputArchiver" should "apply deletion for a streaming job" in {

    // Initialize spark files for this test
    val testCsvFile: File = initializeSparkFiles(jobId2, mappingTaskName)

    // Check whether csv file exists
    testCsvFile.exists() shouldBe true

    // Call archiving function
    fileStreamInputArchiver.applyArchivingOnStreamingJob(testExecutionWithDelete, mappingTaskName)

    // Check whether csv file is deleted
    testCsvFile.exists() shouldBe false

    // Clean test directory
    org.apache.commons.io.FileUtils.deleteDirectory(new File(ToFhirConfig.sparkCheckpointDirectory))
  }

  "FileStreamInputArchiver" should "not apply archiving/deletion for a batch job with archive mode is off" in {

      // Create a test input file
      val inputFile = initializeInputFiles(sourceFolderPath, inputFilePath)

      // Check whether input file exists
      inputFile.exists() shouldBe true

      //Call archiving function
      FileStreamInputArchiver.applyArchivingOnBatchJob(testExecutionWithOff)

      // Check whether input file remains
      inputFile.exists() shouldBe true

      // Clean test directories
      org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(sourceFolderPath).toFile)
  }

  "FileStreamInputArchiver" should "apply archiving for a batch job" in{

    // Create a test input file
    val inputFile = initializeInputFiles(sourceFolderPath, inputFilePath)

    // Find the relative path between the workspace folder and the file to be archived
    val relPath = FileUtils.getPath("").toAbsolutePath.relativize(inputFile.toPath.toAbsolutePath)
    // The relative path is appended to the base archive folder so that the path of the original input file is preserved
    val finalArchivePath = Paths.get(ToFhirConfig.engineConfig.archiveFolder, relPath.toString)

    // Check whether archiving file does not exist and input file exists
    finalArchivePath.toFile.exists() shouldBe false
    inputFile.exists() shouldBe true

    //Call archiving function
    FileStreamInputArchiver.applyArchivingOnBatchJob(testExecution)

    // Check whether archiving file exists and input file is moved
    finalArchivePath.toFile.exists() shouldBe true
    inputFile.exists() shouldBe false

    // Clean test directories
    org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(sourceFolderPath).toFile)
    org.apache.commons.io.FileUtils.deleteDirectory(new File(ToFhirConfig.engineConfig.archiveFolder))
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
    org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(sourceFolderPath).toFile)
  }

  "FileStreamInputArchiver" should "get input files" in {

    val mappingTaskName = "mocked_mappingTask_name"
    val jobId = "mocked_job_id_3"

    // Create a source file to refer location of test.csv
    val sourceFile = getSourceFileFromTestArchiver(jobId, mappingTaskName, "0")

    // Path of test.csv and test2.csv
    val testCsvFile = FileUtils.getPath("test-archiver", "test.csv").toFile
    val test2CsvFile = FileUtils.getPath("test-archiver", "test2.csv").toFile
    // Ensure the parent directories exist, if not, create them
    sourceFile.getParentFile.mkdirs()
    testCsvFile.getParentFile.mkdirs()
    test2CsvFile.getParentFile.mkdirs()
    // Write path of test.csv and test2.csv to the source file
    val sourceWriter = new PrintWriter(sourceFile)
    SparkUtil.writeToSourceFile(sourceWriter, testCsvFile)
    SparkUtil.writeToSourceFile(sourceWriter, test2CsvFile)
    sourceWriter.close()

    // Access getInputFile method of SparkUtil using reflection
    val sparkUtilInstance = SparkUtil
    val methodSymbol = typeOf[SparkUtil.type].decl(TermName("getInputFiles")).asMethod
    val methodMirror = runtimeMirror(getClass.getClassLoader).reflect(sparkUtilInstance)
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

    val mappingTaskName = "mocked_mappingTask_name"
    val jobId = "mocked_job_id_4"

    // Create a source file to refer location of test.csv
    val commitFile = getCommitFileFromTestArchiver(jobId, mappingTaskName, "0")
    val commitFile2 = getCommitFileFromTestArchiver(jobId, mappingTaskName, "1")
    val commitDirectory = new File(SparkUtil.getCommitDirectoryPath(FileUtils.getPath("test-archiver", jobId, mappingTaskName.hashCode.toString)))
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

    // Access getLastCommitOffset method of SparkUtil using reflection
    val SparkUtilInstance = SparkUtil
    val methodSymbol = typeOf[SparkUtil.type].decl(TermName("getLastCommitOffset")).asMethod
    val methodMirror = runtimeMirror(getClass.getClassLoader).reflect(SparkUtilInstance)
    val getLastCommitOffsetMethod = methodMirror.reflectMethod(methodSymbol)

    // Call reflected getLastCommitOffset function
    val result: Int = getLastCommitOffsetMethod(commitDirectory).asInstanceOf[Int]
    // Check whether result is as expected
    result shouldBe 1

    // Clean test directory
    org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath("test-archiver").toFile)
  }

  "FileStreamInputArchiver" should "get last processed offset" in {

    val mappingTaskName = "mocked_mappingTask_name"
    val jobId = "mocked_job_id_5"

    // Get map of processed offset
    val processedOffsetMap = fileStreamInputArchiver.processedOffsets

    // Access getOffsetKey method of FileStreamInputArchiver using reflection
    val FileStreamInputArchiverInstance = FileStreamInputArchiver
    val methodSymbol = typeOf[FileStreamInputArchiver.type].decl(TermName("getOffsetKey")).asMethod
    val methodMirror = runtimeMirror(getClass.getClassLoader).reflect(FileStreamInputArchiverInstance)
    val getOffsetKeyMethod = methodMirror.reflectMethod(methodSymbol)

    // Call reflected getOffsetKey function
    val  lastProccessedOffset = processedOffsetMap.getOrElseUpdate(getOffsetKeyMethod(jobId, mappingTaskName).asInstanceOf[String], -1)
    // Check whether lastProccessedOffset is as expected
    lastProccessedOffset shouldBe -1

    // Update lastProccessedOffset
    processedOffsetMap.put(getOffsetKeyMethod(jobId, mappingTaskName).asInstanceOf[String], 2)

    // Call reflected getOffsetKey function
    val newLastProccessedOffset = processedOffsetMap.getOrElseUpdate(getOffsetKeyMethod(jobId, mappingTaskName).asInstanceOf[String], -1)

    // Check whether lastProccessedOffset is updated
    newLastProccessedOffset shouldBe 2
  }

  /**
   * Creates a dummy input file.
   * @param sourceFolderPath Source folder path for input file
   * @param inputFileName Input file name
   * @return Return input file
   */
  private def initializeInputFiles(sourceFolderPath: String, inputFileName: String): File = {
    // Create a test input file
    val inputFile = FileUtils.getPath(sourceFolderPath, inputFileName).toFile
    inputFile.getParentFile.mkdirs()
    // Write test to input file
    val inputWriter = new PrintWriter(inputFile)
    inputWriter.write("test")
    inputWriter.close()

    inputFile
  }

  /**
   * Creates the following Spark files for the given mapping:
   *  - A commit file
   *  - A source file
   *  - A test csv file
   * @param jobId Job id of the execution.
   * @param mappingTaskName Selected mappingTask name.
   * @return Return test csv file
   */
  private def initializeSparkFiles(jobId: String, mappingTaskName: String): File = {
    // Create a source file to refer location of test.csv
    val sourceFile = getSourceFileFromSparkArchiver(jobId, mappingTaskName, "0")
    // Path of test.csv
    val testCsvFile = Paths.get(ToFhirConfig.sparkCheckpointDirectory, "test.csv").toFile
    // Ensure the parent directories exist, if not, create them
    sourceFile.getParentFile.mkdirs()
    testCsvFile.getParentFile.mkdirs()
    // Write path of test.csv to the source file
    val sourceWriter = new PrintWriter(sourceFile)
    SparkUtil.writeToSourceFile(sourceWriter, testCsvFile)
    sourceWriter.close()

    // Create a test csv
    val testCsvWriter = new PrintWriter(testCsvFile)
    testCsvWriter.write("testColumn1,testColumn2,testColumn3,testColumn4,testColumn5\ntestRow1,testRow2,testRow3,testRow4,testRow5")
    testCsvWriter.close()

    // Create a commit file inorder to start range function in applyArchivingOnStreamingJob
    val commitFile = getCommitFileFromSparkArchiver(jobId, mappingTaskName, "0")
    // Ensure the parent directories exist, if not, create them
    commitFile.getParentFile.mkdirs()
    val commitWriter = new PrintWriter(commitFile)
    commitWriter.write("v1\n{\"nextBatchWatermarkMs\":0}")
    commitWriter.close()

    testCsvFile
  }

  /**
   * Get source file from test-archiver directory.
   *
   * @param jobId           Job id of the execution.
   * @param mappingTaskName Selected mappingTask name.
   * @param fileName        source file name
   * @return Return source file
   */
  def getSourceFileFromTestArchiver(jobId: String, mappingTaskName: String, fileName: String): File = {
    Paths.get(
      SparkUtil.getSourceDirectoryPath(FileUtils.getPath("test-archiver", jobId, mappingTaskName.hashCode.toString)),
      fileName
    ).toFile
  }

  /**
   * Get source file from spark directory.
   *
   * @param jobId           Job id of the execution.
   * @param mappingTaskName Selected mappingTask name.
   * @param fileName        source file name
   * @return Return source file
   */
  def getSourceFileFromSparkArchiver(jobId: String, mappingTaskName: String, fileName: String): File = {
    Paths.get(
      SparkUtil.getSourceDirectoryPath(Paths.get(ToFhirConfig.sparkCheckpointDirectory, jobId, mappingTaskName.hashCode.toString)),
      fileName
    ).toFile
  }

  /**
   * Get commit file from test-archiver directory.
   *
   * @param jobId           Job id of the execution.
   * @param mappingTaskName Selected mappingTask name.
   * @param fileName        commit file name
   * @return Return commit file
   */
  def getCommitFileFromTestArchiver(jobId: String, mappingTaskName: String, fileName: String): File = {
    Paths.get(
      SparkUtil.getCommitDirectoryPath(FileUtils.getPath("test-archiver", jobId, mappingTaskName.hashCode.toString)),
      fileName
    ).toFile
  }

  /**
   * Get commit file from spark directory.
   *
   * @param jobId           Job id of the execution.
   * @param mappingTaskName Selected mapping name.
   * @param fileName        commit file name
   * @return Return commit file
   */
  def getCommitFileFromSparkArchiver(jobId: String, mappingTaskName: String, fileName: String): File = {
    Paths.get(
      SparkUtil.getCommitDirectoryPath(Paths.get(ToFhirConfig.sparkCheckpointDirectory, jobId, mappingTaskName.hashCode.toString)),
      fileName
    ).toFile
  }
}
