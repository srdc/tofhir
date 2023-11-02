package io.tofhir.test.engine.model

import io.tofhir.engine.config.ToFhirConfig
import io.tofhir.engine.model.{FhirMappingJob, FhirMappingJobExecution, FhirRepositorySinkSettings}
import io.tofhir.engine.util.FileUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FhirMappingJobExecutionTest extends AnyFlatSpec with Matchers{

  // Create test execution
  val mappingUrl = "mocked_mapping_url"
  val jobId = "mocked_job_id"
  val testSinkSettings: FhirRepositorySinkSettings = FhirRepositorySinkSettings(fhirRepoUrl = "test")
  val testJob: FhirMappingJob = FhirMappingJob(id = jobId, sinkSettings = testSinkSettings, sourceSettings = Map.empty, mappings = Seq.empty)
  val testExecution: FhirMappingJobExecution = FhirMappingJobExecution(job = testJob)

  "FhirMappingJobExecution" should "get source file" in {
    // Test whether source directory is right
    testExecution.getSourceDirectory(mappingUrl) shouldBe
      FileUtils.getPath(ToFhirConfig.sparkCheckpointDirectory, jobId, mappingUrl.hashCode.toString, "sources", "0").toString
  }

  "FhirMappingJobExecution" should "get commit file" in {
    // Test whether commit directory is right
    testExecution.getCommitDirectory(mappingUrl) shouldBe
      FileUtils.getPath(ToFhirConfig.sparkCheckpointDirectory, jobId, mappingUrl.hashCode.toString, "commits").toString
  }
}