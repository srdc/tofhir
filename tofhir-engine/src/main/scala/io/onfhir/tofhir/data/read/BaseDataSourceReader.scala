package io.onfhir.tofhir.data.read

import io.onfhir.tofhir.model.{DataSourceSettings, FhirMappingSourceContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import java.time.LocalDateTime

/**
 * Base data source reader
 */
abstract class BaseDataSourceReader[T <: FhirMappingSourceContext, S<:DataSourceSettings] {

  /**
   * Read the source data for the given task
   * @param mappingSource   Context/configuration information for mapping source
   * @param sourceSettings  Common settings for source system
   * @param schema          Schema for the source data
   * @param timeRange       Time range for the data to read if given
   * @return
   */
  def read(mappingSource: T, sourceSettings:S, schema: Option[StructType], timeRange: Option[(LocalDateTime, LocalDateTime)] = Option.empty): DataFrame

}


