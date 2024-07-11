package io.tofhir.engine.data.read

import io.tofhir.engine.model.{MappingJobSourceSettings, MappingSourceBinding, FhirServerSource, FhirServerSourceSettings, FileSystemSource, FileSystemSourceSettings, KafkaSource, KafkaSourceSettings, SqlSource, SqlSourceSettings}
import org.apache.spark.sql.SparkSession

/**
 * Factory for data source readers
 */
object DataSourceReaderFactory {

  /**
   * Return appropriate data source reader
   *
   * @param spark                Spark session
   * @param mappingSourceBinding Mapping source binding
   * @return
   */
  def apply[T <: MappingSourceBinding, S<:MappingJobSourceSettings](spark: SparkSession, mappingSourceBinding: T, sourceSettings:S): BaseDataSourceReader[T,S] = {
    (mappingSourceBinding -> sourceSettings) match {
      case (_: FileSystemSource, _:FileSystemSourceSettings) => new FileDataSourceReader(spark).asInstanceOf[BaseDataSourceReader[T,S]]
      case (_: SqlSource, _:SqlSourceSettings) => new SqlSourceReader(spark).asInstanceOf[BaseDataSourceReader[T,S]]
      case (_: KafkaSource, _:KafkaSourceSettings) => new KafkaSourceReader(spark).asInstanceOf[BaseDataSourceReader[T,S]]
      case (_: FhirServerSource, _:FhirServerSourceSettings) => new FhirServerDataSourceReader(spark).asInstanceOf[BaseDataSourceReader[T,S]]
      case _ => throw new NotImplementedError()
    }
  }

}
