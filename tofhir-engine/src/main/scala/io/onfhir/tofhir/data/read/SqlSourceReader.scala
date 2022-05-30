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
    val connectionProperties = new Properties()
    connectionProperties.put("user", mappingSource.settings.username)
    connectionProperties.put("password", mappingSource.settings.password)

    spark.read
      .jdbc(mappingSource.settings.databaseUrl, mappingSource.tableName, connectionProperties)
  }
}
