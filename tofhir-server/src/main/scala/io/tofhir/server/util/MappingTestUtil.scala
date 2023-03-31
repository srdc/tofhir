package io.tofhir.server.util

import io.tofhir.engine.model.{FhirMappingJob, FileSystemSource, FileSystemSourceSettings}
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.config.SparkConfig
import io.tofhir.server.model.{FhirMappingTaskTest, RowSelection}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, rand}

import java.io.{BufferedWriter, File, FileWriter}

object MappingTestUtil {
  /**
   * Find the each data source file with its FhirMappingTask and create a test file by using the row selection configuration
   * @param job used to get dataFolderPath
   * @param fhirMappingTaskTest used to get the FhirMappingTask(s) and row selection configuration
   * @return updated FhirMappingTaskTest with the new test file path
   */
  def createTaskWithSelection(job: FhirMappingJob, fhirMappingTaskTest: FhirMappingTaskTest): FhirMappingTaskTest = {
    fhirMappingTaskTest.copy(
      fhirMappingTask = fhirMappingTaskTest.fhirMappingTask.copy(
        sourceContext = fhirMappingTaskTest.fhirMappingTask.sourceContext.map(map => {
          map._2 match {
            case source: FileSystemSource =>
              val testFile = createTestFileAndWriteSelectedRows(job, source, fhirMappingTaskTest.selection)
              // update the path with the new test file
              (map._1, source.copy(path = testFile.getName))
            case _ => map
          }
        })
      )
    )
  }

  /**
   * Deletes the created source test files after the test is done
   * @param job used to get dataFolderPath
   * @param updatedMappingTaskTest used to get the names of the test files
   */
  def deleteCreatedTestFiles(job: FhirMappingJob, updatedMappingTaskTest: FhirMappingTaskTest): Unit = {
    updatedMappingTaskTest.fhirMappingTask.sourceContext.map(map => {
      map._2 match {
        case source: FileSystemSource =>
          val file: File = FileUtils.getPath(job.sourceSettings.head._2.asInstanceOf[FileSystemSourceSettings].dataFolderPath, source.path).toFile
          file.delete()
        case _ => map
      }
    })
  }

  /**
   * Read the source file to be tested and create a new test file with the selection configuration
   * @param job mapping job used to get dataFolderPath
   * @param source source context of the file, it has
   * @param rowSelection
   * @return
   */
  private def createTestFileAndWriteSelectedRows(job: FhirMappingJob, source: FileSystemSource, rowSelection: RowSelection): File = {
    // get file by using job and source
    val file: File = FileUtils.getPath(job.sourceSettings.head._2.asInstanceOf[FileSystemSourceSettings].dataFolderPath, source.path).toFile
    // create a new test file with suffixed `-test`
    val testFile = new File(file.getParentFile, file.getName.split("\\.").head + "-test." + file.getName.split("\\.").tail.head)
    // read the source file and write the selected rows to the new test file
    source.sourceType match {
      case "csv" =>
        val df = SparkConfig.sparkSession.read.option("header", "true").csv(file.getPath)
        val selectedDF = selectRows(df, rowSelection)
        val rows = selectedDF.collect()
        val str = selectedDF.columns.mkString(",") + ("\n") + rows.map(_.mkString(",")).mkString("\n")
        val bw = new BufferedWriter(new FileWriter(testFile))
        bw.write(str)
        bw.close()
        testFile
      case "json" =>
        val df = SparkConfig.sparkSession.read.json(file.getPath)
        val selectedDF = selectRows(df, rowSelection)
        selectedDF.write.json(testFile.getPath)
        testFile
      case _ => throw new Exception("Unsupported source type: " + source.sourceType)
    }
  }

  /**
   * Apply the row selection to the dataframe
   * @param df dataframe to select rows from
   * @param rowSelection row selection to apply
   * @return dataframe with selected rows
   */
  private def selectRows(df: DataFrame, rowSelection: RowSelection): DataFrame = {
    rowSelection.order match {
      case "start" => df.limit(rowSelection.numberOfRows)
      case "end" => df.withColumn("index", monotonically_increasing_id())
        .orderBy(col("index").desc)
        .drop("index")
        .limit(rowSelection.numberOfRows)
      case "random" => df.orderBy(rand()).limit(rowSelection.numberOfRows)
    }
  }

}
