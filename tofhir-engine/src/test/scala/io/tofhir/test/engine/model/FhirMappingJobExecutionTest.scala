package io.tofhir.test.engine.model

import io.tofhir.engine.model.{FhirMappingJob, FhirMappingJobExecution, FhirRepositorySinkSettings}
import io.tofhir.engine.util.SparkUtil
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Paths

class FhirMappingJobExecutionTest extends AnyFlatSpec with Matchers{

  // Create test execution
  val mappingTaskName = "mocked_mappingTask_name"
  val jobId = "mocked_job_id"
  val testSinkSettings: FhirRepositorySinkSettings = FhirRepositorySinkSettings(fhirRepoUrl = "test")
  val testJob: FhirMappingJob = FhirMappingJob(id = jobId, sinkSettings = testSinkSettings, sourceSettings = Map.empty, mappings = Seq.empty)
  val testExecution: FhirMappingJobExecution = FhirMappingJobExecution(job = testJob)

  "FhirMappingJobExecution" should "get source file" in {
    // Test whether source directory is right
    testExecution.getSourceDirectory(mappingTaskName) shouldBe
      SparkUtil.getSourceDirectoryPath(Paths.get(testExecution.getCheckpointDirectory(mappingTaskName)))
  }

  "FhirMappingJobExecution" should "get commit file" in {
    // Test whether commit directory is right
    testExecution.getCommitDirectory(mappingTaskName) shouldBe
      SparkUtil.getCommitDirectoryPath(Paths.get(testExecution.getCheckpointDirectory(mappingTaskName)))
  }
}
