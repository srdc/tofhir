package io.onfhir.tofhir.data.read

import io.onfhir.tofhir.model.{FhirMappingException, SqlQuerySource, SqlSource}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 *
 * @param spark
 */
class SqlQuerySourceReader(spark:SparkSession) extends BaseDataSourceReader[SqlQuerySource]{

  override def read(mappingSource: SqlQuerySource, schema: StructType): DataFrame = {
    throw FhirMappingException(s"Invalid method call, schema should not be specified for sql source mappings")
  }

  /**
   * Read the source data for the given task
   *
   * @param mappingSource Context/configuration information for mapping source
   * @return
   */
  override def read(mappingSource: SqlQuerySource): DataFrame = {
    spark.read
      .format("jdbc")
      .option("url", mappingSource.settings.databaseUrl)
      .option("dbtable", "(" + mappingSource.query + ") customTable")
      .option("user", mappingSource.settings.username)
      .option("password", mappingSource.settings.password)
      .load()
  }
}
