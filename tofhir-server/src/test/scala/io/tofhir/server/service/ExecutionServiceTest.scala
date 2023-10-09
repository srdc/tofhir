package io.tofhir.server.service

import akka.actor.ActorSystem
import io.tofhir.engine.config.ErrorHandlingType
import io.tofhir.engine.model._
import io.tofhir.server.model.ExecuteJobTask
import io.tofhir.server.service.job.JobFolderRepository
import io.tofhir.server.service.mapping.ProjectMappingFolderRepository
import io.tofhir.server.service.schema.SchemaFolderRepository
import org.apache.commons.io
import org.mockito.MockitoSugar._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.{File, FileOutputStream}
import scala.collection.mutable
import scala.concurrent.ExecutionContext

class ExecutionServiceTest extends AnyWordSpec with Matchers {

  implicit val actorSystem: ActorSystem = ActorSystem("toFhirEngineTest")
  implicit val executionContext: ExecutionContext = actorSystem.getDispatcher

  // FhirMappingJob for test
  val testJob: FhirMappingJob = FhirMappingJob(name = Some("testJob"), sourceSettings = Map.empty, sinkSettings = sinkSettings, mappings = Seq.apply(patientMappingTask),
    dataProcessingSettings = DataProcessingSettings(mappingErrorHandling = ErrorHandlingType.CONTINUE, archiveMode = ArchiveModes.OFF))
  val sinkSettings: FhirSinkSettings = FileSystemSinkSettings(path = "http://example.com/fhir")
  val patientMappingTask: FhirMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/patient-mapping",
    sourceContext = Map("source" -> KafkaSource(topicName = "patients", groupId = "tofhir", startingOffsets = "earliest"))
  )
  val testExecuteJobTask: ExecuteJobTask = ExecuteJobTask(clearCheckpoints = true, Option.empty, Option.empty)

  val mappingRepository: ProjectMappingFolderRepository = mock[ProjectMappingFolderRepository]
  val schemaRepository: SchemaFolderRepository = mock[SchemaFolderRepository]
  val mappingJobRepository: JobFolderRepository = getMockMappingJobRepository

  // the execution service instance for the test
  val executionService: ExecutionService = new ExecutionService(mappingJobRepository, mappingRepository, schemaRepository)

  "The Execution Service" should {
    "should clear checkpoint directory" in {

      val path: String = s"./checkpoint/${testJob.id}/${patientMappingTask.mappingRef.hashCode}"

      // create an example file for test
      val testFile: File = new File(s"${path}/test.txt")
      io.FileUtils.createParentDirectories(testFile)
      val fileOutputStream = new FileOutputStream(testFile)
      fileOutputStream.write("test".getBytes("UTF-8"))
      fileOutputStream.close()

      // check whether file is written to the directory
      val testDirectory: File = new File(path)
      io.FileUtils.sizeOfDirectory(testDirectory) shouldBe > (0L)

      // run job and expect to clear the created directory
      executionService.runJob("testProject", "testJob", Option.empty, Some(testExecuteJobTask)).map(_ =>
        io.FileUtils.sizeOfDirectory(testDirectory) shouldBe 0L
      )
    }
  }

  private def getMockMappingJobRepository: JobFolderRepository = {
    val mockMappingJobRepository: JobFolderRepository = mock[JobFolderRepository]
    when(mockMappingJobRepository.getCachedMappingsJobs).thenReturn(mutable.Map("testProject" -> mutable.Map("testJob" -> testJob)))
  }

}
