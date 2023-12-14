package io.tofhir.server.service

import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.log.server.model.Json4sSupport.formats
import io.tofhir.log.server.service.ExecutionService
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

class ExecutionServiceTest extends AsyncWordSpec with Matchers with BeforeAndAfterAll {

  val executionService: ExecutionService = new ExecutionService

  "The Execution Service" should {
    "should get executions" in {
      executionService.getExecutions("pilot1", "pilot1", Map.empty)
        .map(executions => {
          val executionsData = executions._1
          val count = executions._2
          count shouldEqual 8
          executionsData.length shouldEqual count
          (executionsData.head \ "id").extract[String] shouldEqual "1f47ee28-65b8-48b5-bbd6-347b15254cfb"
          (executionsData.head \ "errorStatus").extract[String] shouldEqual "SUCCESS"
          (executionsData.head \ "mappingUrls").extract[Seq[String]].length shouldEqual 4

          (executionsData(5) \ "id").extract[String] shouldEqual "4df92d34-7712-47a7-8677-15339797c554"
          (executionsData(5) \ "errorStatus").extract[String] shouldEqual "PARTIAL_SUCCESS"
          (executionsData(5) \ "mappingUrls").extract[Seq[String]].length shouldEqual 6

          (executionsData(5) \ "mappingUrls")(1).extract[String] shouldEqual "https://aiccelerate.eu/fhir/mappings/pilot1/health-behavior-assessment-mapping"
        })
    }

    "should get executions filtered by a date range" in {
      val queryParams = Map("dateBefore" -> "2023-12-06", "dateAfter" -> "2023-12-03")
      executionService.getExecutions("pilot1", "pilot1", queryParams)
        .map(executions => {
          val executionsData = executions._1
          val count = executions._2
          count shouldEqual 1
          executionsData.length shouldEqual count
          (executionsData.head \ "id").extract[String] shouldEqual "1f47ee28-65b8-48b5-bbd6-347b15254cfb"
          (executionsData.head \ "errorStatus").extract[String] shouldEqual "SUCCESS"
          (executionsData.head \ "mappingUrls").extract[Seq[String]].length shouldEqual 4
          (executionsData.head \ "mappingUrls")(1).extract[String] shouldEqual "https://aiccelerate.eu/fhir/mappings/pilot1/vital-signs-mapping"
        })
    }

    "should not get any execution when page number is greater than the maximum" in {
      val queryParams = Map("dateBefore" -> "2023-12-06", "dateAfter" -> "2023-12-03", "rowsPerPage" -> "4", "page" -> "3")
      executionService.getExecutions("pilot1", "pilot1", queryParams)
        .map(executions => {
          val executionsData = executions._1
          val count = executions._2
          count shouldEqual 0
          executionsData.length shouldEqual count
          })
    }

    "should get executions filtered by error status" in {
      val queryParams = Map("errorStatuses" -> "SUCCESS")
      executionService.getExecutions("pilot1", "pilot1", queryParams)
        .map(executions => {
          val executionsData = executions._1
          val count = executions._2
          count shouldEqual 3
          executionsData.length shouldEqual count
          (executionsData(1) \ "id").extract[String] shouldEqual "57bb4431-5d58-44d5-b525-a838c0cfa21c"
          (executionsData(1) \ "errorStatus").extract[String] shouldEqual "SUCCESS"
          (executionsData(1) \ "startTime").extract[String] shouldEqual "2023-12-01T14:37:31.348+03:00"
          (executionsData(1) \ "mappingUrls").extract[Seq[String]].length shouldEqual 1
          (executionsData(1) \ "mappingUrls")(0).extract[String] shouldEqual "https://aiccelerate.eu/fhir/mappings/pilot1/other-observation-mapping"
        })
    }

    "should get execution by id" in {
      executionService.getExecutionById("pilot1", "pilot1", "4df92d34-7712-47a7-8677-15339797c554")
        .map(execution => {
          (execution \ "id").extract[String] shouldEqual "4df92d34-7712-47a7-8677-15339797c554"
          (execution \ "errorStatus").extract[String] shouldEqual "PARTIAL_SUCCESS"
          (execution \ "mappingUrls").extract[Seq[String]].length shouldEqual 6
          (execution \ "mappingUrls")(0).extract[String] shouldEqual "https://aiccelerate.eu/fhir/mappings/pilot1/preoperative-risks-mapping"
        })
    }

    "should get execution logs by execution id" in {
      executionService.getExecutionLogs("4df92d34-7712-47a7-8677-15339797c554")
        .map(logs => {
          logs.length shouldEqual 11

          // iterate over logs and check project ids/job ids/execution ids are correct
          (0 to 10).foreach(i => {
            (logs(i) \ "projectId").extract[String] shouldEqual "pilot1"
            (logs(i) \ "executionId").extract[String] shouldEqual "4df92d34-7712-47a7-8677-15339797c554"
            (logs(i) \ "jobId").extract[String] shouldEqual "pilot1"
          })

          // check the first log -> STARTED
          (logs.head \ "result").extract[String] shouldEqual "STARTED"
          (logs.head \ "mappingUrl").extract[String] shouldEqual "https://aiccelerate.eu/fhir/mappings/pilot1/medication-administration-mapping"
          (logs(1) \ "result").extract[String] shouldEqual "SUCCESS"
          (logs(1) \ "numOfFhirResources").extract[Int] shouldEqual 5
          (logs(1) \ "mappingUrl").extract[String] shouldEqual "https://aiccelerate.eu/fhir/mappings/pilot1/medication-administration-mapping"

        })
    }
  }
}
