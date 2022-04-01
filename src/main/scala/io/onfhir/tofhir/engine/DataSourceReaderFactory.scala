package io.onfhir.tofhir.engine

import io.onfhir.tofhir.model.{DataSourceSettings, FhirMappingTask, FileSystemSourceSettings}
import org.apache.spark.sql.SparkSession

object DataSourceReaderFactory {

  /**
   *
   * @param dataSourceSettings
   * @return
   */
  def apply[T<:FhirMappingTask](spark:SparkSession, dataSourceSettings:DataSourceSettings[T]):BaseDataSourceReader[T] = {
    dataSourceSettings match {
      case fs:FileSystemSourceSettings => new FileDataSourceReader(spark, fs)
      case _ => throw new NotImplementedError()
    }
  }

}
