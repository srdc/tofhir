package io.ignifyr.engine.spi

import io.ignifyr.engine.data.read.BaseDataSourceReader
import io.ignifyr.engine.model.{MappingJobSourceSettings, MappingSourceBinding}
import org.apache.spark.sql.SparkSession

/**
 * Contributes a data-source reader for one source-binding type.
 *
 * The binding/settings model classes live either in the engine (the community sources) or in the module
 * itself, which then contributes them through [[IgnifyrExtension.jobModelClasses]]. For an engine class a job
 * JSON referencing this source parses everywhere, and when no connector is registered for it the engine raises
 * a [[MissingConnectorException]] at read time; for a module class the job parses only where the module is
 * installed.
 */
trait SourceConnector {

  /** Stable identifier, e.g. "file", "sql", "kafka". */
  def id: String

  /** Binding model class this connector reads (the registry lookup key). */
  def bindingClass: Class[_ <: MappingSourceBinding]

  /** Source-settings model class this connector expects. */
  def settingsClass: Class[_ <: MappingJobSourceSettings]

  /**
   * Construct the reader for a given Spark session. The concrete reader is parameterized on the
   * binding/settings types; callers cast to their statically-known types, mirroring the previous
   * `DataSourceReaderFactory` behaviour.
   */
  def createReader(spark: SparkSession): BaseDataSourceReader[_, _]
}
