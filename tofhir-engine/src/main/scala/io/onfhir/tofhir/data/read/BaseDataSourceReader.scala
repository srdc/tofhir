package io.onfhir.tofhir.data.read

import io.onfhir.tofhir.model.FhirMappingSourceContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

/**
 * Base data source reader
 */
abstract class BaseDataSourceReader[T<:FhirMappingSourceContext] {

  /**
   * Read the source data for the given task
   * @param mappingSource   Context/configuration information for mapping source
   * @param schema          Schema for the source
   * @return
   */
  def read(mappingSource:T, schema:StructType):DataFrame

  /**
   * Read the source data for the given task
   * @param mappingSource   Context/configuration information for mapping source
   * @return
   */
  def read(mappingSource:T):DataFrame

}


