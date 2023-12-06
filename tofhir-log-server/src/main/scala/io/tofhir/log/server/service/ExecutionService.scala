package io.tofhir.log.server.service

import com.typesafe.scalalogging.LazyLogging
import io.tofhir.log.server.config.SparkConfig
import io.tofhir.log.server.model.ResourceNotFound
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{col, lit, to_date, to_timestamp, when}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoders, Row}
import org.json4s.JsonAST.{JObject, JValue}
import org.json4s.jackson.JsonMethods
import org.json4s.{JArray, JString}

import java.text.SimpleDateFormat
import java.util.Date
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * Service to handle all execution related operations
 * E.g. Run a mapping job, run a mapping task, run a test resource creation, get execution logs
 *
 */
class ExecutionService() extends LazyLogging {

  /**
   * Returns the logs of mapping tasks ran in the given execution.
   *
   * @param executionId the identifier of mapping job execution.
   * @return the logs of mapping tasks
   * */
  def getExecutionLogs(executionId: String): Future[Seq[JValue]] = {
    Future {
      // read logs/tofhir-mappings.log file
      val dataFrame = SparkConfig.sparkSession.read.json("logs/tofhir-mappings.log")
      // handle the case where no job has been run yet which makes the data frame empty
      if (dataFrame.isEmpty) {
        Seq.empty
      }
      else {
        // Get mapping tasks logs for the given execution. ProjectId field is not null for selecting mappingTasksLogs, filter out row error logs.
        var mappingTasksLogs = dataFrame.filter(s"executionId = '$executionId' and projectId is not null")

        // mapping tasks without input data are markes as 'STARTED', map them to 'FAILURE'
        mappingTasksLogs = mappingTasksLogs.withColumn("result",
          when(col("level") === "ERROR", "FAILURE")
            .otherwise(col("result"))
        )

        // Handle the case where the job has not been run yet, which makes the data frame empty
        if (mappingTasksLogs.isEmpty) {
          Seq.empty
        } else {
          // Collect mapping tasks logs for matching with mappingUrl field of row error logs
          var mappingTasksLogsData = mappingTasksLogs.collect()

          // Get row error logs for the given execution. ProjectId field is null for selecting row error logs, filter out mappingTasksLogs.
          var rowErrorLogs = dataFrame.filter(s"executionId = '$executionId' and projectId is null")

          // Check whether there is any row error
          if(!rowErrorLogs.isEmpty){

            // Select needed columns from row error logs
            rowErrorLogs = rowErrorLogs.select(List("errorCode", "errorDesc", "message", "mappingUrl").map(col):_*)

            // Group row error logs by mapping url
            val rowErrorLogsGroupedByMappingUrl = rowErrorLogs.groupByKey(row => row.get(row.fieldIndex("mappingUrl")).toString)(Encoders.STRING)

            // Add row error details to mapping tasks logs if any error occurred while executing the mapping task.
            val mappingTasksErrorLogsWithRowErrorLogs = rowErrorLogsGroupedByMappingUrl.mapGroups((mappingUrl, rowError) => {
              // Find the related mapping task log to given mapping url
              val mappingTaskLog = mappingTasksLogsData.filter(row => row.getAs[String]("mappingUrl") == mappingUrl)
              // Append row error logs to the related mapping task log
              Row.fromSeq(Row.unapplySeq(mappingTaskLog.head).get :+ rowError.toSeq)
            })(
              // Define a new schema for the resulting rows and create an encoder for it. We will add a "error_logs" column to mapping tasks logs that contains related error logs.
              RowEncoder(mappingTasksLogs.schema.add("error_logs", ArrayType(
                new StructType()
                  .add("errorCode", StringType)
                  .add("errorDesc", StringType)
                  .add("message", StringType)
                  .add("mappingUrl", StringType)
              ))
              )
            )

            // Build a map for updated mapping tasks logs (mappingUrl -> mapping logs with errors)
            val updatedMappingTasksLogsMap = mappingTasksErrorLogsWithRowErrorLogs.collect().map(mappingLogsWithErrors =>
              (mappingLogsWithErrors.getAs[String]("mappingUrl"), mappingLogsWithErrors.getAs[String]("@timestamp")) -> mappingLogsWithErrors
            ).toMap

            // Replace mapping task logs if it is in the map
            mappingTasksLogsData = mappingTasksLogsData.map(mappingTaskLog =>
              updatedMappingTasksLogsMap.getOrElse((mappingTaskLog.getAs[String]("mappingUrl"), mappingTaskLog.getAs[String]("@timestamp")), mappingTaskLog))

          }

          // return json objects for mapping tasks logs
          mappingTasksLogsData.map(row => {
            JsonMethods.parse(row.json).asInstanceOf[JObject]
          })
        }
      }
    }
  }

  /**
   * Returns the list of mapping job executions. It extracts the logs from {@link logs/ tofhir - mappings.log} file for
   * the given mapping job and groups them by their execution id and returns a single log for each execution. Further,
   * it applies the pagination to the resulting execution logs.
   *
   * @param projectId   project id the job belongs to
   * @param jobId       job id
   * @param queryParams parameters to filter results such as paging
   * @return a tuple as follows
   *         first element is the execution logs of mapping job as a JSON array. It returns an empty array if the job has not been run before.
   *         second element is the total number of executions without applying any filters i.e. query params
   * @throws ResourceNotFound when mapping job does not exist
   */
  def getExecutions(projectId: String, jobId: String, queryParams: Map[String, String]): Future[(Seq[JValue], Long, Long)] = {
    // retrieve the job to validate its existence
    val dateBefore = queryParams.getOrElse("dateBefore", null)
    val dateAfter = queryParams.getOrElse("dateAfter", null)
    val errorStatuses = queryParams.getOrElse("errorStatus", null)

    Future {
      // read logs/tofhir-mappings.log file
      val dataFrame = SparkConfig.sparkSession.read.json("logs/tofhir-mappings.log")
      // handle the case where no job has been run yet which makes the data frame empty
      if (dataFrame.isEmpty) {
        (Seq.empty, 0, 0)
      }
      else {
        val jobRuns = dataFrame.filter(s"jobId = '$jobId' and projectId = '$projectId'")
        // handle the case where the job has not been run yet which makes the data frame empty
        if (jobRuns.isEmpty) {
          (Seq.empty, 0, 0)
        } else {
          // group logs by execution id
          val jobRunsGroupedByExecutionId = jobRuns.groupByKey(row => row.get(row.fieldIndex("executionId")).toString)(Encoders.STRING)
          // get execution logs
          val executionLogs = jobRunsGroupedByExecutionId.mapGroups((key, values) => {
            // keeps the rows belonging to this execution
            val rows: Seq[Row] = values.toSeq
            // Extract values from the "mappingUrl" column using direct attribute access
            val mappingUrls = rows.map(_.getAs[String]("mappingUrl")).distinct
            // use the timestamp of first one, which is ran first, as timestamp of execution
            val timestamp = rows.head.get(rows.head.fieldIndex("@timestamp")).toString
            // Check if there is a row with result other than STARTED
            val results: Seq[String] = rows.map(row => row.get(row.fieldIndex("result")).toString)
            val status: String = ExecutionService.getErrorStatusOfExecution(results)
            Row.fromSeq(Seq(key, mappingUrls, timestamp, status))
          })(RowEncoder(StructType(
            StructField("id", StringType) ::
              StructField("mappingUrls", ArrayType(StringType)) ::
              StructField("startTime", StringType) ::
              StructField("errorStatus", StringType) :: Nil
          )))
          // page size is 10, handle pagination
          val total = executionLogs.count()
          val numOfPages = Math.ceil(total.toDouble / 10).toInt
          val page = queryParams.getOrElse("page", "1").toInt
          // handle the case where requested page does not exist
          if (page > numOfPages) {
            (Seq.empty, 0, 0)
          } else {
            val start = (page - 1) * 10
            val end = Math.min(start + 10, total.toInt)

            var filteredLogs = executionLogs
            // Filter according to error status of the execution
            if(Option(errorStatuses).nonEmpty){
              filteredLogs = filteredLogs.filter(col("errorStatus").isin(errorStatuses.split(","): _*))
            }

            // Filter according to start date
            if(Option(dateAfter).nonEmpty){
              filteredLogs = filteredLogs.filter(col("startTime") > dateAfter)
            }
            if (Option(dateBefore).nonEmpty) {
              filteredLogs = filteredLogs.filter(col("startTime") < dateBefore)
            }

            // sort the executions by latest to oldest
            val paginatedLogs = filteredLogs.sort(executionLogs.col("startTime").desc).collect().slice(start, end)

            // Retrieve the running executions for the given job
            (paginatedLogs.map(row => {
              JsonMethods.parse(row.json).asInstanceOf[JObject]
            }), total.toInt, filteredLogs.count())
          }
        }
      }
    }
  }

  /**
   * Returns the execution logs for a specific execution ID.
   *
   * @param projectId    project id the job belongs to
   * @param jobId        job id
   * @param executionId  execution id
   * @return the execution summary as a JSON object
   */
  def getExecutionById(projectId: String, jobId: String, executionId: String): Future[JObject] = {
    // Retrieve the job to validate its existence
    Future {
      // Read logs/tofhir-mappings.log file
      val dataFrame = SparkConfig.sparkSession.read.json("logs/tofhir-mappings.log")
      // Filter logs by job and execution ID
      val filteredLogs = dataFrame.filter(s"jobId = '$jobId' and projectId = '$projectId' and executionId = '$executionId'")
      // Check if any logs exist for the given execution
      if (filteredLogs.isEmpty) { // execution not found, return response with 404 status code
        throw ResourceNotFound("Execution does not exists.", s"An execution with id $executionId does not exists.")
      } else {
        // Extract values from the "mappingUrl" column using direct attribute access
        val mappingUrls = filteredLogs.select("mappingUrl").distinct().collect().map(_.getString(0)).toList
        // Use the timestamp of the first log as the execution timestamp
        val timestamp = filteredLogs.select("@timestamp").first().getString(0)
        // Determine the status based on the success count
        val results: Seq[String] = filteredLogs.select("result").collect().map(_.getString(0)).toSeq
        val status: String = ExecutionService.getErrorStatusOfExecution(results)
        // Create a JSON object representing the execution
        val executionJson = JObject(
          "id" -> JString(executionId),
          "mappingUrls" -> JArray(mappingUrls.map(JString)),
          "startTime" -> JString(timestamp),
          "errorStatus" -> JString(status)
        )
        executionJson
      }
    }
  }

}

object ExecutionService {

  /**
   * Determines the error status of the execution based on the results of the mapping tasks.
   * @param results
   * @return
   */
  private def getErrorStatusOfExecution(results: Seq[String]): String = {
    if (results.exists(_ == "PARTIAL_SUCCESS")) {
      "PARTIAL_SUCCESS"
    } else {
      // success > 0 and failure = 0 means success
      // success > 0 and failure > 0 means partial success
      // success = 0 and failure > 0 means failure
      // success = 0 and failure = 0 means started
      val successCount = results.count(_ == "SUCCESS")
      val failureCount = results.count(_ == "FAILURE")
      if (successCount > 0 && failureCount == 0) {
        "SUCCESS"
      } else if (successCount > 0 && failureCount > 0) {
        "PARTIAL_SUCCESS"
      } else if (failureCount > 0) {
        "FAILURE"
      } else {
        "STARTED"
      }
    }
  }
}



