package io.tofhir.engine.data.read

import com.typesafe.scalalogging.Logger
import io.tofhir.engine.model.{FileSystemSource, FileSystemSourceSettings, SourceFileFormats}
import io.tofhir.engine.util.FileUtils
import org.apache.spark.sql.functions.{input_file_name, udf}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File
import java.net.URI
import java.time.LocalDateTime
import scala.collection.mutable

/**
 * Reader from file system
 *
 * @param spark Spark session
 */
class FileDataSourceReader(spark: SparkSession) extends BaseDataSourceReader[FileSystemSource, FileSystemSourceSettings] {

  private val logger: Logger = Logger(this.getClass)
  /**
   * Read the source data
   *
   * @param mappingSource Context/configuration information for mapping source
   * @param schema        Optional schema for the source
   * @param limit         Limit the number of rows to read
   * @param jobId         The identifier of mapping job which executes the mapping
   * @return
   * @throws IllegalArgumentException If the path is not a directory for streaming jobs.
   * @throws NotImplementedError      If the specified source format is not implemented.
   */
  override def read(mappingSource: FileSystemSource, sourceSettings:FileSystemSourceSettings, schema: Option[StructType], timeRange: Option[(LocalDateTime, LocalDateTime)], limit: Option[Int] = Option.empty,jobId: Option[String] = Option.empty): DataFrame = {
    // get the format of the file
    val sourceType = mappingSource.inferFileFormat
    // determine the final path
    // if it is a Hadoop path (starts with "hdfs://"), construct the URI directly without adding the context path
    val finalPath = if (sourceSettings.dataFolderPath.startsWith("hdfs://")) {
      new URI(s"${sourceSettings.dataFolderPath.stripSuffix("/")}/${mappingSource.path.stripPrefix("/")}").toString
    } else {
      FileUtils.getPath(sourceSettings.dataFolderPath, mappingSource.path).toAbsolutePath.toString
    }
    // validate whether the provided path is a directory when streaming is enabled in the source settings
    if(sourceSettings.asStream && !new File(finalPath).isDirectory){
      throw new IllegalArgumentException(s"$finalPath is not a directory. For streaming job, you should provide a directory.")
    }

    val isDistinct = mappingSource.options.get("distinct").contains("true")

    // keeps the names of processed files by Spark
    val processedFiles: mutable.HashSet[String] =mutable.HashSet.empty
    //Based on source type
    val resultDf = sourceType match {
        case SourceFileFormats.CSV | SourceFileFormats.TSV =>
          val updatedOptions = sourceType match {
            case SourceFileFormats.TSV =>
              // If the file format is tsv, use tab (\t) as separator by default if it is not set explicitly
              mappingSource.options +
                ("sep" -> mappingSource.options.getOrElse("sep", "\\t"),
                // use *.tsv as pathGlobFilter by default if it is not set explicitly to ignore files without tsv extension
                "pathGlobFilter" -> mappingSource.options.getOrElse("pathGlobFilter", s"*.${SourceFileFormats.TSV}"))
            case SourceFileFormats.CSV =>
              mappingSource.options +
                // use *.csv as pathGlobFilter by default if it is not set explicitly to ignore files without csv extension
                ("pathGlobFilter" -> mappingSource.options.getOrElse("pathGlobFilter", s"*.${SourceFileFormats.CSV}"))
          }

          //Options that we infer for csv
          val inferSchema = schema.isEmpty || mappingSource.preprocessSql.isDefined
          val csvSchema = if(mappingSource.preprocessSql.isDefined) None else schema
          //val enforceSchema = schema.isDefined
          val includeHeader = mappingSource.options.get("header").forall(_ == "true")
          //Other options except header, inferSchema and enforceSchema
          val otherOptions = updatedOptions.filterNot(o => o._1 == "header" || o._1 == "inferSchema" || o._1 == "enforceSchema")
          if(sourceSettings.asStream)
            spark.readStream
              .option("enforceSchema", false) //Enforce schema should be false (See https://spark.apache.org/docs/latest/sql-data-sources-csv.html)
              .option("header", includeHeader)
              .option("inferSchema", inferSchema)
              .options(otherOptions)
              .schema(csvSchema.orNull)
              .csv(finalPath)
              // add a dummy column called 'filename' using a udf function to print a log when the data reading is
              // started for a file.
              .withColumn("filename",logStartOfDataReading(processedFiles,logger = logger,jobId =  jobId)(input_file_name))
          else
            spark.read
              .option("enforceSchema", false) //Enforce schema should be false
              .option("header", includeHeader)
              .option("inferSchema", inferSchema)
              .options(otherOptions)
              .schema(csvSchema.orNull)
              .csv(finalPath)
        case SourceFileFormats.JSON =>
          if(sourceSettings.asStream)
            spark.readStream.options(mappingSource.options).schema(schema.orNull).json(finalPath)
              // add a dummy column called 'filename' to print a log when the data reading is started for a file
              .withColumn("filename", logStartOfDataReading(processedFiles, logger = logger, jobId = jobId)(input_file_name))
          else
            spark.read.options(mappingSource.options).schema(schema.orNull).json(finalPath)
        case SourceFileFormats.PARQUET =>
          if(sourceSettings.asStream)
            spark.readStream.options(mappingSource.options).schema(schema.orNull).parquet(finalPath)
              // add a dummy column called 'filename' to print a log when the data reading is started for a file
              .withColumn("filename", logStartOfDataReading(processedFiles, logger = logger, jobId = jobId)(input_file_name))
          else
            spark.read.options(mappingSource.options).schema(schema.orNull).parquet(finalPath)
        case _ => throw new NotImplementedError()
      }
    if(isDistinct)
      resultDf.distinct()
    else
      resultDf
  }

  /**
   * A user-defined function i.e. udf to print a log when data reading is started for a file. udf takes the
   * name of input file being read and returns it after logging a message to indicate that data reading is started and
   * it may take a while. It makes use of the given processedFiles set to decide whether to print a log. If it does not
   * contain the file name i.e. Spark just started to process it, the log is printed.
   * @param processedFiles The set of file names processed by Spark
   * @param logger  Logger instance
   * @param jobId   The identifier of mapping job which executes the mapping
   * @return a user-defined function to print a log when data reading is started for a file
   * */
  private def logStartOfDataReading(processedFiles: mutable.HashSet[String], logger: Logger, jobId: Option[String]) = udf((fileName: String) => {
    // if the file is not processed yet, print the log and add it to the processed files set
    if (!processedFiles.contains(fileName)) {
      logger.info(s"Reading data from $fileName for the mapping job ${jobId.getOrElse("")}. This may take a while...")
      // add it to the set
      processedFiles.add(fileName)
    }
    fileName
  })
}
