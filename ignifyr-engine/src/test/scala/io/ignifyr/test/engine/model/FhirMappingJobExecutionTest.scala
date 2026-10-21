package io.ignifyr.test.engine.model

import io.ignifyr.engine.model.{
  FhirMappingJob,
  FhirMappingJobExecution,
  FhirMappingJobResult,
  FhirRepositorySinkSettings,
  FileSystemSinkSettings,
  SinkContentTypes
}
import io.ignifyr.engine.util.SparkUtil
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Paths

class FhirMappingJobExecutionTest extends AnyFlatSpec with Matchers {

  // Create test execution
  val mappingTaskName = "mocked_mappingTask_name"
  val jobId = "mocked_job_id"
  val testSinkSettings: FhirRepositorySinkSettings = FhirRepositorySinkSettings(fhirRepoUrl = "test")
  val testJob: FhirMappingJob =
    FhirMappingJob(id = jobId, sinkSettings = testSinkSettings, sourceSettings = Map.empty, mappings = Seq.empty)
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

  "FhirMappingJobExecution" should "validate the mapped resources only when the sink asks for it" in {
    // A FHIR server sink validates on its own and never asks for it
    testExecution.validateResources shouldBe false
    FhirMappingJobExecution(job =
      testJob.copy(sinkSettings = FileSystemSinkSettings("./out", SinkContentTypes.NDJSON))
    ).validateResources shouldBe false
    FhirMappingJobExecution(job =
      testJob.copy(sinkSettings = FileSystemSinkSettings("./out", SinkContentTypes.NDJSON, validateResources = true))
    ).validateResources shouldBe true
  }

  "FhirMappingJobExecution" should "write its results unless skipping the write is asked for" in {
    testExecution.isWriteSkipped shouldBe false
    FhirMappingJobExecution(job = testJob, skipWrite = true).isWriteSkipped shouldBe true
  }

  // The Kibana dashboards read `isWriteSkipped` off every result event to tell a written execution from a skipped one
  "FhirMappingJobResult" should "carry whether the write is skipped in its log marker" in {
    def markerOf(execution: FhirMappingJobExecution) =
      FhirMappingJobResult(execution, Some(mappingTaskName)).toMapMarker.getMap
    markerOf(testExecution).get("isWriteSkipped") shouldBe false
    markerOf(testExecution.copy(isWriteSkipped = true)).get("isWriteSkipped") shouldBe true
  }

  it should "not report the resources of a skipped write as written" in {
    val result =
      FhirMappingJobResult(testExecution.copy(isWriteSkipped = true), Some(mappingTaskName), chunkResult = false)
    result.toString should include("write skipped")
    result.toString should not include "Written"
  }
}
