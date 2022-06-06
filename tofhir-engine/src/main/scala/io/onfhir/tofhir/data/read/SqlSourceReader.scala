package io.onfhir.tofhir.data.read

import io.onfhir.tofhir.model.{FhirMappingException, SqlSource}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

/**
 *
 * @param spark
 */
class SqlSourceReader(spark:SparkSession) extends BaseDataSourceReader[SqlSource]{

  override def read(mappingSource: SqlSource, schema: StructType): DataFrame = {
    throw FhirMappingException(s"Invalid method call, schema should not be specified for sql source mappings")
  }

  /**
   * Read the source data for the given task
   *
   * @param mappingSource Context/configuration information for mapping source
   * @return
   */
  override def read(mappingSource: SqlSource): DataFrame = {
    if (mappingSource.query.isDefined && mappingSource.tableName.isDefined){
      throw FhirMappingException(s"Both table name: ${mappingSource.tableName.get} and query: ${mappingSource.query.get} should not be specified at the same time.")
    }

    var dbTable: String = ""
    if (mappingSource.query.isDefined){
      // As in spark jdbc read docs, instead of a full table you could also use a subquery in parentheses.
      dbTable = s"( ${mappingSource.query.get} ) queryGeneratedTable"
    } else{
      dbTable = mappingSource.tableName.get
    }

    spark.read
      .format("jdbc")
      .option("url", mappingSource.settings.databaseUrl)
      .option("dbtable", dbTable)
      .option("user", mappingSource.settings.username)
      .option("password", mappingSource.settings.password)
      .load()
  }
}
