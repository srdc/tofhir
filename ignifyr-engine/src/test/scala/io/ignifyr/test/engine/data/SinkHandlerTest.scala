package io.ignifyr.test.engine.data

import io.ignifyr.engine.config.IgnifyrConfig
import io.ignifyr.engine.data.write.{BaseSinkWriter, SinkHandler}
import io.ignifyr.engine.execution.log.ExecutionLogger
import io.ignifyr.engine.model._
import io.ignifyr.engine.spi.{
  MappedResourceValidator,
  MissingCapabilityException,
  ResourceValidatorProvider,
  ValidationIssue
}
import io.ignifyr.test.engine.data.SinkHandlerTest.StubValidatorProvider
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.util.CollectionAccumulator
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import java.time.Instant

/**
 * `SinkHandler` decides what actually reaches a sink: it splits the mapping results into invalid inputs,
 * mapping errors and mapped resources, and hands only the last group to the writer. A row landing in the
 * wrong group is silent — an error row written as if it were a resource, or a good resource dropped —
 * so the split is asserted here rather than through a sink that would hide it.
 */
class SinkHandlerTest extends AnyFlatSpec with Matchers {

  private val sparkSession: SparkSession = IgnifyrConfig.sparkSession

  /** Captures the dataset it is handed instead of writing it anywhere. */
  private class CapturingWriter extends BaseSinkWriter(FileSystemSinkSettings("./out", SinkContentTypes.NDJSON)) {
    var written: Seq[FhirMappingResult] = Seq.empty
    var problemsBeforeWrite: Seq[FhirMappingResult] = Seq.empty
    var writeCalls: Int = 0
    override def write(
        spark: SparkSession,
        df: Dataset[FhirMappingResult],
        problemsAccumulator: CollectionAccumulator[FhirMappingResult]
    ): Unit = {
      writeCalls += 1
      written = df.collect().toSeq
      problemsBeforeWrite = problemsAccumulator.value.toArray(Array.empty[FhirMappingResult]).toSeq
    }
    override def validate(): Unit = ()
  }

  private def result(
      source: String,
      mappedResource: Option[String] = None,
      error: Option[FhirMappingError] = None
  ): FhirMappingResult =
    FhirMappingResult(
      jobId = "job-1",
      mappingTaskName = "task-1",
      timestamp = Timestamp.from(Instant.now()),
      source = source,
      mappedFhirResource = mappedResource.map(resource => MappedFhirResource(mappedResource = Some(resource))),
      error = error
    )

  private val execution: FhirMappingJobExecution = FhirMappingJobExecution(
    id = "execution-1",
    job = FhirMappingJob(
      id = "job-1",
      sourceSettings = Map.empty,
      sinkSettings = FileSystemSinkSettings("./out", SinkContentTypes.NDJSON),
      mappings = Seq.empty,
      dataProcessingSettings = DataProcessingSettings() // saveErroneousRecords = false
    )
  )

  /*
   * `SinkHandler` reports each chunk through `ExecutionLogger`, which keeps per-execution state and
   * looks the execution up by id — so a chunk can only be logged for an execution that was already
   * logged as STARTED. The launcher always does that first; the call here stands in for it, and
   * omitting it is what a caller wiring up a new execution path would trip over.
   */
  ExecutionLogger.logExecutionStatus(execution, FhirMappingJobResult.STARTED)

  private def write(results: FhirMappingResult*): Seq[FhirMappingResult] = {
    import sparkSession.implicits._
    val writer = new CapturingWriter
    SinkHandler.writeMappingResult(sparkSession, execution, "task-1", results.toSeq.toDS(), writer)
    writer.written
  }

  "writeMappingResult" should "hand the mapped resources to the writer" in {
    val mapped = result("row-1", mappedResource = Some("""{"resourceType":"Patient"}"""))
    write(mapped).map(_.source) shouldBe Seq("row-1")
  }

  it should "keep an invalid input away from the writer" in {
    val invalid = result("bad-row", error = Some(FhirMappingError(FhirMappingErrorCodes.INVALID_INPUT, "missing pid")))
    write(invalid) shouldBe empty
  }

  it should "keep a mapping error away from the writer" in {
    val failed = result("row-2", error = Some(FhirMappingError(FhirMappingErrorCodes.MAPPING_ERROR, "bad expression")))
    write(failed) shouldBe empty
  }

  it should "write only the mapped resources of a mixed batch" in {
    val written = write(
      result("ok-1", mappedResource = Some("""{"resourceType":"Patient"}""")),
      result("bad", error = Some(FhirMappingError(FhirMappingErrorCodes.INVALID_INPUT, "missing pid"))),
      result("failed", error = Some(FhirMappingError(FhirMappingErrorCodes.MAPPING_ERROR, "bad expression"))),
      result("ok-2", mappedResource = Some("""{"resourceType":"Observation"}"""))
    )
    written.map(_.source) should contain theSameElementsAs Seq("ok-1", "ok-2")
  }

  // A row with no payload and no error is nothing to write — a mapping whose precondition excluded it.
  it should "skip a result that carries neither a payload nor an error" in {
    write(result("skipped")) shouldBe empty
  }

  it should "call the writer even when there is nothing to write" in {
    import sparkSession.implicits._
    val writer = new CapturingWriter
    SinkHandler.writeMappingResult(sparkSession, execution, "task-1", Seq.empty[FhirMappingResult].toDS(), writer)
    writer.writeCalls shouldBe 1
  }

  // An execution that skips writing still maps and logs, but its mapped resources never reach the writer.
  it should "not call the writer when the execution skips writing" in {
    import sparkSession.implicits._
    val writer = new CapturingWriter
    SinkHandler.writeMappingResult(
      sparkSession,
      execution.copy(isWriteSkipped = true),
      "task-1",
      Seq(result("row-1", mappedResource = Some("""{"resourceType":"Patient"}"""))).toDS(),
      writer
    )
    writer.writeCalls shouldBe 0
    writer.written shouldBe empty
  }

  private val validatingExecution = execution.copy(validateResources = true)

  private def writeValidated(results: FhirMappingResult*): CapturingWriter = {
    import sparkSession.implicits._
    val writer = new CapturingWriter
    SinkHandler.writeMappingResult(
      sparkSession,
      validatingExecution,
      "task-1",
      results.toSeq.toDS(),
      writer,
      Some(StubValidatorProvider)
    )
    writer
  }

  "writeMappingResult with resource validation" should "write only the resources without validation errors" in {
    val writer = writeValidated(
      result("ok", mappedResource = Some("""{"resourceType":"Patient"}""")),
      result("invalid", mappedResource = Some("""{"resourceType":"Patient","invalid":true}""")),
      result("warned", mappedResource = Some("""{"resourceType":"Patient","warn":true}"""))
    )
    writer.written.map(_.source) should contain theSameElementsAs Seq("ok", "warned")
  }

  it should "report an invalid resource as invalid_resource with its issues, before the write" in {
    val writer = writeValidated(result("invalid", mappedResource = Some("""{"resourceType":"Patient","invalid":true}""")))
    writer.problemsBeforeWrite.map(_.source) shouldBe Seq("invalid")
    val error = writer.problemsBeforeWrite.head.error.get
    error.code shouldBe FhirMappingErrorCodes.INVALID_RESOURCE
    error.expression.get should include("Patient.invalid")
  }

  it should "reject a resource whose validation fails with an exception" in {
    val writer = writeValidated(result("boom", mappedResource = Some("""{"resourceType":"Patient","boom":true}""")))
    writer.written shouldBe empty
    writer.problemsBeforeWrite.head.error.get.expression.get should include("could not be validated")
  }

  it should "fail clearly when no resource validator is installed" in {
    import sparkSession.implicits._
    val thrown = the[MissingCapabilityException] thrownBy SinkHandler.writeMappingResult(
      sparkSession,
      validatingExecution,
      "task-1",
      Seq(result("row-1", mappedResource = Some("""{"resourceType":"Patient"}"""))).toDS(),
      new CapturingWriter,
      None
    )
    thrown.getMessage should include("ignifyr-validation")
  }

  it should "fail fast at job start when no resource validator is installed" in {
    // The community engine has no resource validator module on its test classpath
    a[MissingCapabilityException] should be thrownBy SinkHandler.requireResourceValidator(validatingExecution)
    noException should be thrownBy SinkHandler.requireResourceValidator(execution)
  }
}

object SinkHandlerTest {

  /**
   * Validator stub: a resource with an `invalid` field has an error, one with a `warn` field a warning, and one with a
   * `boom` field makes the validator throw. Top-level, so that it serializes to the Spark executors.
   */
  object StubValidatorProvider extends ResourceValidatorProvider with MappedResourceValidator {
    override def validatorFor(
        mappingJobExecution: FhirMappingJobExecution,
        mappingTaskName: String
    ): MappedResourceValidator = this

    override def validate(mappedResource: String): Seq[ValidationIssue] =
      if (mappedResource.contains("\"boom\"")) throw new IllegalStateException("validator failure")
      else
        Seq(
          if (mappedResource.contains("\"invalid\""))
            Some(ValidationIssue("error", "invalid", Some("bad"), Seq("Patient.invalid")))
          else None,
          if (mappedResource.contains("\"warn\""))
            Some(ValidationIssue("warning", "invalid", Some("odd"), Seq("Patient.warn")))
          else None
        ).flatten
  }
}
