package io.onfhir.tofhir.engine

import io.onfhir.tofhir.model.{DataSourceSettings, FhirMappingTask}
import org.apache.spark.sql.DataFrame

/**
 * Base data source reader
 * @param dss   Data source reader settings for the data source
 */
abstract class BaseDataSourceReader[T<:FhirMappingTask](dss:DataSourceSettings[T]) {

  /**
   * Read the source data for the given task
   * @param mappingTask Given mapping task
   * @return
   */
  def read(mappingTask:T):DataFrame

}


