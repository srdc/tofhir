package io.tofhir.test.engine.data.write

import io.tofhir.engine.config.ToFhirConfig
import io.tofhir.engine.data.write.{BaseFhirWriter, SinkHandler}
import io.tofhir.engine.model.{DataProcessingSettings, FhirMappingJob, FhirMappingJobExecution, FhirMappingResult, FileSystemSourceSettings}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.mockito.ArgumentMatchers
import org.mockito.MockitoSugar._
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.Timestamp
import java.time.Instant
import scala.language.postfixOps

class SinkHandlerTest extends AnyFlatSpec {

  val sparkSession: SparkSession = ToFhirConfig.sparkSession

  "SinkHandler" should "continue processing subsequent chunks for streaming queries after a chunk throws an exception" in {//
    // A mock
    val mockJob: FhirMappingJob = mock[FhirMappingJob]
    when(mockJob.id).thenReturn("jobId")
    when(mockJob.dataProcessingSettings).thenReturn(DataProcessingSettings.apply())
    when(mockJob.sourceSettings).thenReturn(Map("0" -> FileSystemSourceSettings.apply("name", "sourceUri", "dataFolderPath")))

    val execution: FhirMappingJobExecution = FhirMappingJobExecution("executionId", "projectId", mockJob)

    // Create a Spark stream generating mapping results
    import sparkSession.implicits._
    val df = sparkSession.readStream
      .format("rate") // Generate timestamp and value tuples
      .option("rowsPerSecond", 1)
      .load()
      .coalesce(1)
      .map(_ => { // Map the generated rows to some dummy mapping result
        FhirMappingResult("someId", "someUrl", Timestamp.from(Instant.now()), "")
      })

    // Configure the mock writer such that it would throw an exception for the first chunk but not for the subsequent chunks
    var chunkCount = 0
    val mockWriter: BaseFhirWriter = mock[BaseFhirWriter]
    when(mockWriter.write(ArgumentMatchers.any(), ArgumentMatchers.argThat[Dataset[FhirMappingResult]]({
      case _ =>
        if (chunkCount == 0) {
          chunkCount = chunkCount + 1
          true
        } else {
          false
        }
    }), ArgumentMatchers.any())).thenThrow(new Exception())

    // Start streaming
    val streamingQuery = SinkHandler.writeStream(sparkSession, execution, df, mockWriter, "someMappingTaskName")

    // Wait for data generation for 5 seconds and then terminate the query
    streamingQuery.awaitTermination(5000)
    streamingQuery.stop()
  }
}
