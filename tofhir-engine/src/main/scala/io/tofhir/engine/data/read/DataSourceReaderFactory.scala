package io.tofhir.engine.data.read

import io.tofhir.engine.model.{DataSourceSettings, FhirMappingSourceContext, FhirServerSource, FhirServerSourceSettings, FileSystemSource, FileSystemSourceSettings, KafkaSource, KafkaSourceSettings, SqlSource, SqlSourceSettings}
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
  def apply[T <: FhirMappingSourceContext, S<:DataSourceSettings](spark: SparkSession, mappingSourceContext: T, sourceSettings:S): BaseDataSourceReader[T,S] = {
    (mappingSourceContext -> sourceSettings) match {
      case (_: FileSystemSource, _:FileSystemSourceSettings) => new FileDataSourceReader(spark).asInstanceOf[BaseDataSourceReader[T,S]]
      case (_: SqlSource, _:SqlSourceSettings) => new SqlSourceReader(spark).asInstanceOf[BaseDataSourceReader[T,S]]
      case (_: KafkaSource, _:KafkaSourceSettings) => new KafkaSourceReader(spark).asInstanceOf[BaseDataSourceReader[T,S]]
      case (_: FhirServerSource, _:FhirServerSourceSettings) => new FhirServerDataSourceReader(spark).asInstanceOf[BaseDataSourceReader[T,S]]
      case _ => throw new NotImplementedError()
    }
  }

}
