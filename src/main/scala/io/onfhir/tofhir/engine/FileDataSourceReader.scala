package io.onfhir.tofhir.engine

import io.onfhir.tofhir.model.{FhirMappingFromFileSystemTask, FileSystemSourceSettings}
import org.apache.spark.sql.DataFrame

/**
 * Reader from file system
 * @param fsss  File system source settings
 */
class FileDataSourceReader(fsss:FileSystemSourceSettings) extends BaseDataSourceReader[FhirMappingFromFileSystemTask, FileSystemSourceSettings](fsss) {
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
