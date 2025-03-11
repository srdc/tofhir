package io.tofhir.engine.data.read

import com.typesafe.scalalogging.Logger
import io.tofhir.engine.model.exception.FhirMappingException
import io.tofhir.engine.model.{SqlSource, SqlSourceSettings}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

/**
 *
 * @param spark
 */
class SqlSourceReader(spark: SparkSession) extends BaseDataSourceReader[SqlSource, SqlSourceSettings] {

  private val logger: Logger = Logger(this.getClass)

  /**
   * Read the source data from relational database systems through SQL
   *
   * @param mappingSourceBinding     Configuration information for the mapping source
   * @param mappingJobSourceSettings Common settings for the source system
   * @param schema                   Schema for the source data
   * @param timeRange                Time range for the data to read if given
   * @param jobId                    The identifier of mapping job which executes the mapping
   * @return
   */
  override def read(mappingSourceBinding: SqlSource, mappingJobSourceSettings: SqlSourceSettings, schema: Option[StructType],
                    timeRange: Option[(LocalDateTime, LocalDateTime)] = Option.empty, jobId: Option[String]): DataFrame = {

    if (mappingSourceBinding.tableName.isDefined && mappingSourceBinding.query.isDefined) {
      throw FhirMappingException(s"Both table name: ${mappingSourceBinding.tableName.get} and query: ${mappingSourceBinding.query.get} should not be specified at the same time.")
    }
    if (mappingSourceBinding.tableName.isEmpty && mappingSourceBinding.query.isEmpty) {
      throw FhirMappingException(s"Both table name and query cannot be empty at the same time. One of them must be provided.")
    }

    if (schema.isDefined) {
      logger.debug("There is a schema definition for the SqlSource, but I cannot enforce it while reading from the database. Hence, ignoring...")
    }

    // As in spark jdbc read docs, instead of a full table you could also use a sub-query in parentheses.
    val dbTable: String = mappingSourceBinding.tableName.getOrElse({
      var query: String = mappingSourceBinding.query.get
      if (timeRange.isDefined) {
        val (fromTs, toTs) = timeRange.get
        query = query.replace("$fromTs", "'" + fromTs.toString + "'").replace("$toTs", "'" + toTs.toString + "'")
      }
      s"( $query ) queryGeneratedTable"
    })

    spark.read
      .format("jdbc")
      .option("url", mappingJobSourceSettings.databaseUrl)
      .option("dbtable", dbTable)
      .option("user", mappingJobSourceSettings.username)
      .option("password", mappingJobSourceSettings.password)
      .options(mappingSourceBinding.options)
      .load()
  }
}
