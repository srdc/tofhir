package io.tofhir.engine.data.read

import com.typesafe.scalalogging.Logger
import io.tofhir.engine.model.{FileSystemSource, FileSystemSourceSettings, SourceFileFormats}
import io.tofhir.engine.util.FileUtils
import org.apache.spark.sql.functions.{input_file_name, udf}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

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
   */
  override def read(mappingSource: FileSystemSource, sourceSettings:FileSystemSourceSettings, schema: Option[StructType], timeRange: Option[(LocalDateTime, LocalDateTime)], limit: Option[Int] = Option.empty,jobId: Option[String] = Option.empty): DataFrame = {
    val finalPath = FileUtils.getPath(sourceSettings.dataFolderPath, mappingSource.path).toAbsolutePath.toString

    val isDistinct = mappingSource.options.get("distinct").contains("true")

    //index of the row being read by Spark
    var rowIndex: Integer = 0
    //Based on source type
    val resultDf = mappingSource.sourceType match {
        case SourceFileFormats.CSV | SourceFileFormats.TSV =>
          val updatedOptions = mappingSource.sourceType match {
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
              // started for a file. indexFn function, which is a parameter of logStartOfDataReading function, increments
              // rowIndex and returns it for each row
              .withColumn("filename",logStartOfDataReading(indexFn = () => {
                rowIndex += 1
                rowIndex
              },logger = logger,jobId =  jobId)(input_file_name))
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
              // indexFn function, which is a parameter of logStartOfDataReading function, increments rowIndex and
              // returns it for each row
              .withColumn("filename", logStartOfDataReading(indexFn = () => {
                rowIndex += 1
                rowIndex
              }, logger = logger, jobId = jobId)(input_file_name))
          else
            spark.read.options(mappingSource.options).schema(schema.orNull).json(finalPath)
        case SourceFileFormats.PARQUET =>
          if(sourceSettings.asStream)
            spark.readStream.options(mappingSource.options).schema(schema.orNull).parquet(finalPath)
              // add a dummy column called 'filename' to print a log when the data reading is started for a file
              // indexFn function, which is a parameter of logStartOfDataReading function, increments rowIndex and
              // returns it for each row
              .withColumn("filename", logStartOfDataReading(indexFn = () => {
                rowIndex += 1
                rowIndex
              }, logger = logger, jobId = jobId)(input_file_name))
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
   * it may take a while. It makes use of the given indexFn function to decide whether to print a log. If it returns 'one'
   * i.e. the first record, the log is printed.
   * @param indexFn A function which returns the index of current row which is being read by Spark
   * @param logger  Logger instance
   * @param jobId   The identifier of mapping job which executes the mapping
   * @return a user-defined function to print a log when data reading is started for a file
   * */
  private def logStartOfDataReading(indexFn: () => Integer, logger: Logger, jobId: Option[String]) = udf((fileName: String) => {
    val index = indexFn()
    if (index == 1) {
      logger.info(s"Reading data from $fileName for the mapping job ${jobId.getOrElse("")}. This may take a while...")
    }
    fileName
  })
}
