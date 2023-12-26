package io.tofhir.server.service

import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.model.FhirMappingJobResult
import io.tofhir.log.server.model.Json4sSupport.formats
import io.tofhir.log.server.service.ExecutionService
import org.json4s.JsonAST.JArray
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

/**
 * Tests for the ExecutionService.
 *
 * This suite focuses on testing the functionality of the ExecutionService, which works with the log file provided
 * in the test/resources/log-sample.log file. The log file includes records for the following scenarios:
 *  - An execution of a mapping job with one mapping, which fails due to an incorrect FHIR Repository URL.
 *  - An execution of a mapping job with one mapping, which completes successfully.
 */
class ExecutionServiceTest extends AsyncWordSpec with Matchers with BeforeAndAfterAll {

  val executionService: ExecutionService = new ExecutionService
  // The identifier for the project
  private val projectId = "pilot1"
  // The identifier for the specific job
  private val jobId = "pilot1-preop"
  // The identifier for the first mapping job execution
  private val firstMappingJobExecutionId = "b9d60a61-6d0b-4ea1-9c4e-84e343334b14"
  // The identifier for the second mapping job execution
  private val secondMappingJobExecutionId = "70503f99-8aea-4b4f-8b1b-6dbc3d5509fe"

  "The Execution Service" should {

    "should get mapping job executions" in {
      executionService.getExecutions(projectId, jobId, Map.empty)
        .map(executions => {
          val executionsData = executions._1
          val count = executions._2
          count shouldEqual 2
          executionsData.length shouldEqual count
          (executionsData.head \ "id").extract[String] shouldEqual secondMappingJobExecutionId
          (executionsData.head \ "errorStatus").extract[String] shouldEqual FhirMappingJobResult.SUCCESS
          (executionsData.head \ "mappingUrls").extract[Seq[String]].length shouldEqual 1

          (executionsData(1) \ "id").extract[String] shouldEqual firstMappingJobExecutionId
          (executionsData(1) \ "errorStatus").extract[String] shouldEqual FhirMappingJobResult.FAILURE
          (executionsData(1) \ "mappingUrls").extract[Seq[String]].length shouldEqual 1
        })
    }

    "should get executions filtered by a date range" in {
      val queryParams = Map("dateBefore" -> "2023-12-27", "dateAfter" -> "2023-12-26")
      executionService.getExecutions(projectId, jobId, queryParams)
        .map(executions => {
          val executionsData = executions._1
          val count = executions._2
          count shouldEqual 1
          executionsData.length shouldEqual count
          (executionsData.head \ "id").extract[String] shouldEqual secondMappingJobExecutionId
          (executionsData.head \ "errorStatus").extract[String] shouldEqual FhirMappingJobResult.SUCCESS
          (executionsData.head \ "mappingUrls").extract[Seq[String]].length shouldEqual 1
          (executionsData.head \ "mappingUrls")(0).extract[String] shouldEqual "https://aiccelerate.eu/fhir/mappings/pilot1/patient-mapping"
        })
    }

    "should not get any execution when page number is greater than the maximum" in {
      val queryParams = Map("dateBefore" -> "2023-12-27", "dateAfter" -> "2023-12-03", "rowsPerPage" -> "4", "page" -> "3")
      executionService.getExecutions(projectId, jobId, queryParams)
        .map(executions => {
          val executionsData = executions._1
          val count = executions._2
          count shouldEqual 0
          executionsData.length shouldEqual count
        })
    }

    "should get executions filtered by error status" in {
      val queryParams = Map("errorStatuses" -> FhirMappingJobResult.SUCCESS)
      executionService.getExecutions(projectId, jobId, queryParams)
        .map(executions => {
          val executionsData = executions._1
          val count = executions._2
          count shouldEqual 1
          executionsData.length shouldEqual count
          (executionsData.head \ "id").extract[String] shouldEqual secondMappingJobExecutionId
          (executionsData.head \ "errorStatus").extract[String] shouldEqual FhirMappingJobResult.SUCCESS
          (executionsData.head \ "mappingUrls").extract[Seq[String]].length shouldEqual 1
        })
    }

    "should get execution by id" in {
      executionService.getExecutionById(projectId, jobId, secondMappingJobExecutionId)
        .map(execution => {
          (execution \ "id").extract[String] shouldEqual secondMappingJobExecutionId
          (execution \ "errorStatus").extract[String] shouldEqual FhirMappingJobResult.SUCCESS
          (execution \ "mappingUrls").extract[Seq[String]].length shouldEqual 1
          (execution \ "mappingUrls")(0).extract[String] shouldEqual "https://aiccelerate.eu/fhir/mappings/pilot1/patient-mapping"
        })
    }

    "should get execution logs by execution id" in {
      executionService.getExecutionLogs(firstMappingJobExecutionId)
        .map(logs => {
          logs.length shouldEqual 2

          // check the first log -> STARTED
          (logs.head \ "result").extract[String] shouldEqual FhirMappingJobResult.STARTED
          (logs.head \ "error_logs").extract[JArray].arr.size shouldEqual 10

          // check the second log -> FAILURE
          (logs(1) \ "result").extract[String] shouldEqual FhirMappingJobResult.FAILURE
          (logs(1) \ "numOfFailedWrites").extract[String] shouldEqual "10"
        })
    }
  }
}
