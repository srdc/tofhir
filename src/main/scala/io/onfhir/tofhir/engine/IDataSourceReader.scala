package io.onfhir.tofhir.engine

import io.onfhir.tofhir.model.{FhirMappingFromFileSystemTask, FhirMappingTask}
import org.apache.spark.sql.DataFrame

trait IDataSourceReader[T<:FhirMappingTask] {

  /**
   * Read the source data for the given task
   * @param mappingTask
   * @return
   */
  def read(mappingTask:T):DataFrame

}

class FileDataSourceReader extends IDataSourceReader[FhirMappingFromFileSystemTask] {
  /**
   * Read the source data for the given task
   *
   * @param mappingTask
   * @return
   */
  override def read(mappingTask: FhirMappingFromFileSystemTask): DataFrame = {
    throw new NotImplementedError()
  }
}

