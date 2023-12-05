package io.tofhir.server.service

import io.tofhir.engine.Execution.actorSystem.dispatcher
import org.apache.commons.io.FileUtils

import java.nio.file.Paths
import io.tofhir.server.model.Json4sSupport.formats
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
          (executionsData.head \ "mappingUrls").extract[Seq[String]].length shouldEqual 21

          (executionsData(5) \ "id").extract[String] shouldEqual "4df92d34-7712-47a7-8677-15339797c554"
          (executionsData(5) \ "errorStatus").extract[String] shouldEqual "PARTIAL_SUCCESS"
          (executionsData(5) \ "mappingUrls").extract[Seq[String]].length shouldEqual 21

          (executionsData(5) \ "mappingUrls")(1).extract[String] shouldEqual "https://aiccelerate.eu/fhir/mappings/pilot1/practitioner-mapping"
        })
    }

    "should get execution by id" in {
      executionService.getExecutionById("pilot1", "pilot1", "4df92d34-7712-47a7-8677-15339797c554")
        .map(execution => {
          (execution \ "id").extract[String] shouldEqual "4df92d34-7712-47a7-8677-15339797c554"
          (execution \ "errorStatus").extract[String] shouldEqual "PARTIAL_SUCCESS"
          (execution \ "mappingUrls").extract[Seq[String]].length shouldEqual 21
          (execution \ "mappingUrls")(0).extract[String] shouldEqual "https://aiccelerate.eu/fhir/mappings/pilot1/patient-reported-conditions-mapping"
        })
    }

    "should get execution logs" in {
      executionService.getExecutionLogs("4df92d34-7712-47a7-8677-15339797c554")
        .map(logs => {
          logs.length shouldEqual 42

          // iterate over logs and check project ids/job ids/execution ids are correct
          (0 to 41).foreach(i => {
            (logs(i) \ "projectId").extract[String] shouldEqual "pilot1"
            (logs(i) \ "executionId").extract[String] shouldEqual "4df92d34-7712-47a7-8677-15339797c554"
            (logs(i) \ "jobId").extract[String] shouldEqual "pilot1"
          })

          // check the first log -> STARTED
          (logs(0) \ "result").extract[String] shouldEqual "STARTED"
          (logs(0) \ "mappingUrl").extract[String] shouldEqual "https://aiccelerate.eu/fhir/mappings/pilot1/patient-mapping"
          (logs(1) \ "result").extract[String] shouldEqual "SUCCESS"
          (logs(1) \ "numOfFhirResources").extract[Int] shouldEqual 10
          (logs(1) \ "mappingUrl").extract[String] shouldEqual "https://aiccelerate.eu/fhir/mappings/pilot1/patient-mapping"

        })
    }
  }

  override def beforeAll(): Unit = {
    // remove log files if exists
    val logDir = io.tofhir.engine.util.FileUtils.getPath("logs/").toFile
    if (logDir.exists && logDir.isDirectory) {
      val files = logDir.listFiles
      files.foreach(f => f.delete)
    }
    // move log file to logs folder by changing its name to tofhir-mappings.log
    val logFile = Paths.get(getClass.getResource("/log-sample.log").toURI).toFile
    val logFileDest = io.tofhir.engine.util.FileUtils.getPath("logs/tofhir-mappings.log").toFile
    FileUtils.copyFile(logFile, logFileDest)

  }
}
