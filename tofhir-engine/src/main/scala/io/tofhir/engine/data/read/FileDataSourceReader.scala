package io.tofhir.engine.data.read

import com.typesafe.scalalogging.Logger
import io.tofhir.engine.model.{FileSystemSource, FileSystemSourceSettings, SourceContentTypes}
import io.tofhir.engine.util.{FileUtils, SparkUtil}
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
   * Read the source data from file system
   *
   * @param mappingSourceBinding     Configuration information for the mapping source
   * @param mappingJobSourceSettings Common settings for the source system
   * @param schema                   Optional schema for the source
   * @param timeRange                Time range for the data to read if given
   * @param jobId                    The identifier of mapping job which executes the mapping
   * @return
   * @throws IllegalArgumentException If the path is not a directory for streaming jobs.
   * @throws NotImplementedError      If the specified source format is not implemented.
   */
  override def read(mappingSourceBinding: FileSystemSource, mappingJobSourceSettings: FileSystemSourceSettings, schema: Option[StructType],
                    timeRange: Option[(LocalDateTime, LocalDateTime)] = Option.empty, jobId: Option[String] = Option.empty): DataFrame = {
    // get the content type for the file
    val contentType = mappingSourceBinding.contentType
    // check whether it is a zip file
    val isZipFile = mappingSourceBinding.path.endsWith(".zip")
    // determine the final path
    // if it is a Hadoop path (starts with "hdfs://"), construct the URI directly without adding the context path
    val finalPath = if (mappingJobSourceSettings.dataFolderPath.startsWith("hdfs://")) {
      new URI(s"${mappingJobSourceSettings.dataFolderPath.stripSuffix("/")}/${mappingSourceBinding.path.stripPrefix("/")}").toString
    } else {
      FileUtils.getPath(mappingJobSourceSettings.dataFolderPath, mappingSourceBinding.path).toAbsolutePath.toString
    }
    // validate whether the provided path is a directory when streaming is enabled in the source settings
    if (mappingJobSourceSettings.asStream && !new File(finalPath).isDirectory) {
      throw new IllegalArgumentException(s"$finalPath is not a directory. For streaming job, you should provide a directory.")
    }

    val isDistinct = mappingSourceBinding.options.get("distinct").contains("true")

    // keeps the names of processed files by Spark
    val processedFiles: mutable.HashSet[String] = mutable.HashSet.empty
    //Based on source type
    val resultDf = contentType match {
      case SourceContentTypes.CSV | SourceContentTypes.TSV =>
        val updatedOptions = contentType match {
          case SourceContentTypes.TSV =>
            // If the file format is tsv, use tab (\t) as separator by default if it is not set explicitly
            mappingSourceBinding.options +
              ("sep" -> mappingSourceBinding.options.getOrElse("sep", "\\t"))
          case SourceContentTypes.CSV => mappingSourceBinding.options
        }

        //Options that we infer for csv
        val inferSchema = schema.isEmpty || mappingSourceBinding.preprocessSql.isDefined
        val csvSchema = if (mappingSourceBinding.preprocessSql.isDefined) None else schema
        //val enforceSchema = schema.isDefined
        val includeHeader = mappingSourceBinding.options.get("header").forall(_ == "true")
        //Other options except header, inferSchema and enforceSchema
        val otherOptions = updatedOptions.filterNot(o => o._1 == "header" || o._1 == "inferSchema" || o._1 == "enforceSchema")
        if (mappingJobSourceSettings.asStream) {
          spark.readStream
            .option("enforceSchema", false) //Enforce schema should be false (See https://spark.apache.org/docs/latest/sql-data-sources-csv.html)
            .option("header", includeHeader)
            .option("inferSchema", inferSchema)
            .options(otherOptions)
            .schema(csvSchema.orNull)
            .csv(finalPath)
            // add a dummy column called 'filename' using a udf function to print a log when the data reading is
            // started for a file.
            .withColumn("filename", logStartOfDataReading(processedFiles, logger = logger, jobId = jobId)(input_file_name))
        } else if (isZipFile) {
          val unzippedFileContents = SparkUtil.readZip(finalPath, spark)
          spark.read
            .option("enforceSchema", false) //Enforce schema should be false
            .option("header", includeHeader)
            .option("inferSchema", inferSchema)
            .options(otherOptions)
            .schema(csvSchema.orNull)
            .csv(unzippedFileContents)
        } else
          spark.read
            .option("enforceSchema", false) //Enforce schema should be false
            .option("header", includeHeader)
            .option("inferSchema", inferSchema)
            .options(otherOptions)
            .schema(csvSchema.orNull)
            .csv(finalPath)
      // assume that each line in the txt files contains a separate JSON object.
      case SourceContentTypes.JSON | SourceContentTypes.NDJSON =>
        if (mappingJobSourceSettings.asStream) {
          // schema cannot be inferred for streaming so let's infer it ourselves
          val inferredSchema = spark.read.options(mappingSourceBinding.options).json(finalPath).schema
          spark.readStream.options(mappingSourceBinding.options).schema(inferredSchema).json(finalPath)
            // add a dummy column called 'filename' to print a log when the data reading is started for a file
            .withColumn("filename", logStartOfDataReading(processedFiles, logger = logger, jobId = jobId)(input_file_name))
        }
        else if (isZipFile) {
          val unzippedFileContents = SparkUtil.readZip(finalPath, spark)
          spark.read.options(mappingSourceBinding.options).json(unzippedFileContents)
        }
        else
          spark.read.options(mappingSourceBinding.options).json(finalPath)
      case SourceContentTypes.PARQUET =>
        if (mappingJobSourceSettings.asStream)
          spark.readStream.options(mappingSourceBinding.options).schema(schema.orNull).parquet(finalPath)
            // add a dummy column called 'filename' to print a log when the data reading is started for a file
            .withColumn("filename", logStartOfDataReading(processedFiles, logger = logger, jobId = jobId)(input_file_name))
        else
          spark.read.options(mappingSourceBinding.options).schema(schema.orNull).parquet(finalPath)
      case _ => throw new NotImplementedError()
    }

    if (isDistinct) resultDf.distinct() else resultDf

  }

  /**
   * A user-defined function i.e. udf to print a log when data reading is started for a file. udf takes the
   * name of input file being read and returns it after logging a message to indicate that data reading is started and
   * it may take a while. It makes use of the given processedFiles set to decide whether to print a log. If it does not
   * contain the file name i.e. Spark just started to process it, the log is printed.
   *
   * @param processedFiles The set of file names processed by Spark
   * @param logger         Logger instance
   * @param jobId          The identifier of mapping job which executes the mapping
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
