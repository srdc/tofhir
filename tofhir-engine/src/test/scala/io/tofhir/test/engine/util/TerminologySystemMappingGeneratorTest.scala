package io.tofhir.test.engine.util

import io.tofhir.engine.model.{FhirMappingJob, FhirMappingJobExecution, FhirRepositorySinkSettings}
import io.tofhir.engine.util.SparkUtil
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Paths

class TerminologySystemMappingGeneratorTest extends AnyFlatSpec with Matchers{

  // Create test execution
  val mappingUrl = "mocked_mapping_url"
  val jobId = "mocked_job_id"
  val testSinkSettings: FhirRepositorySinkSettings = FhirRepositorySinkSettings(fhirRepoUrl = "test")
  val testJob: FhirMappingJob = FhirMappingJob(id = jobId, sinkSettings = testSinkSettings, sourceSettings = Map.empty, mappings = Seq.empty)
  val testExecution: FhirMappingJobExecution = FhirMappingJobExecution(job = testJob)

  "FhirMappingJobExecution" should "get source file" in {

  }

}
