package io.onfhir.tofhir.data.read

import com.typesafe.scalalogging.Logger
import io.onfhir.tofhir.model.{FhirMappingException, SqlSource}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

/**
 *
 * @param spark
 */
class SqlSourceReader(spark: SparkSession) extends BaseDataSourceReader[SqlSource] {

  private val logger: Logger = Logger(this.getClass)

  /**
   * Read the source data for the given task
   *
   * @param mappingSource Context/configuration information for mapping source
   * @return
   */
  override def read(mappingSource: SqlSource, schema: Option[StructType], timeRange: Option[(LocalDateTime, LocalDateTime)]): DataFrame = {
    if (mappingSource.tableName.isDefined && mappingSource.query.isDefined) {
      throw FhirMappingException(s"Both table name: ${mappingSource.tableName.get} and query: ${mappingSource.query.get} should not be specified at the same time.")
    }
    if (mappingSource.tableName.isEmpty && mappingSource.query.isEmpty) {
      throw FhirMappingException(s"Both table name: ${mappingSource.tableName.get} and query: ${mappingSource.query.get} cannot be empty at the same time. One of them must be provided.")
    }

    if(schema.isDefined) {
      logger.debug("There is a schema definitions for the SqlSource, but I cannot enforce it while reading from the database. Hence, ignoring...")
    }

    // As in spark jdbc read docs, instead of a full table you could also use a sub-query in parentheses.
    val dbTable: String = mappingSource.tableName.getOrElse({
      if (timeRange.isDefined) {
        val (fromTs, toTs) = timeRange.get
        val query = mappingSource.query.get.replace("$fromTs", "'" + fromTs.toString + "'").replace("$toTs", "'" + toTs.toString + "'")
        s"( $query ) queryGeneratedTable"
      } else {
        s"( ${mappingSource.query.get} ) queryGeneratedTable"
      }
    })

    spark.read
      .format("jdbc")
      .option("url", mappingSource.settings.databaseUrl)
      .option("dbtable", dbTable)
      .option("user", mappingSource.settings.username)
      .option("password", mappingSource.settings.password)
      .load()
  }
}
