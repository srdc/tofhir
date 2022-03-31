package io.onfhir.tofhir.engine

import io.onfhir.tofhir.model.{DataSourceSettings, FhirMappingFromFileSystemTask, FhirMappingTask, FileSystemSourceSettings}
import org.apache.spark.sql.DataFrame

/**
 * Base data source reader
 * @param dss   Data source reader settings for the data source
 * @tparam T    Mapping task type to support
 * @tparam S    Settings type to support
 */
abstract class BaseDataSourceReader[T<:FhirMappingTask, S<:DataSourceSettings](dss:S) {

  /**
   * Read the source data for the given task
   * @param mappingTask Given mapping task
   * @return
   */
  def read(mappingTask:T):DataFrame

}


