package io.tofhir.engine.data.read

import io.tofhir.engine.model.{MappingJobSourceSettings, MappingSourceBinding}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import java.time.LocalDateTime

/**
 * Base data source reader
 */
abstract class BaseDataSourceReader[T <: MappingSourceBinding, S <: MappingJobSourceSettings] {

  /**
   * Read the source data for the given task
   *
   * @param mappingSourceBinding     Configuration information for the mapping source
   * @param mappingJobSourceSettings Common settings for the source system
   * @param schema                   Schema for the source data
   * @param timeRange                Time range for the data to read if given
   * @param jobId                    The identifier of mapping job which executes the mapping
   * @return
   */
  def read(mappingSourceBinding: T, mappingJobSourceSettings: S, schema: Option[StructType], timeRange: Option[(LocalDateTime, LocalDateTime)], jobId: Option[String]): DataFrame

  /**
   * Whether this reader needs a data type validation for columns after reading the source
   */
  val needTypeValidation: Boolean = false

  /**
   * Whether this reader needs cardinality validation for columns after reading the source
   */
  val needCardinalityValidation: Boolean = true

}


