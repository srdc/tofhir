package io.onfhir.tofhir.data.read

import io.onfhir.tofhir.model.{FhirMappingSourceContext, FileSystemSource, SqlQuerySource, SqlSource}
import org.apache.spark.sql.SparkSession

/**
 * Factory for data source readers
 */
object DataSourceReaderFactory {

  /**
   * Return appropriate data source reader
   *
   * @param spark                Spark session
   * @param mappingSourceContext Mapping source context
   * @return
   */
  def apply[T <: FhirMappingSourceContext](spark: SparkSession, mappingSourceContext: T): BaseDataSourceReader[T] = {
    mappingSourceContext match {
      case _: FileSystemSource => new FileDataSourceReader(spark).asInstanceOf[BaseDataSourceReader[T]]
      case _: SqlSource => new SqlSourceReader(spark).asInstanceOf[BaseDataSourceReader[T]]
      case _: SqlQuerySource => new SqlQuerySourceReader(spark).asInstanceOf[BaseDataSourceReader[T]]
      case _ => throw new NotImplementedError()
    }
  }

}
