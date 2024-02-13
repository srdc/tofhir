package io.tofhir.engine.data.write

import io.tofhir.engine.model.{FhirMappingResult, FhirRepositorySinkSettings, FhirSinkSettings, FileSystemSinkSettings}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.util.CollectionAccumulator

/**
 * Base class for FHIR resource writer
 *
 * @param sinkSettings
 */
abstract class BaseFhirWriter(sinkSettings: FhirSinkSettings) extends Serializable {
  /**
   * Write the data frame of json serialized FHIR resources to given sink (e.g. FHIR repository)
   *
   * @param df
   */
  def write(sparkSession: SparkSession, df: Dataset[FhirMappingResult], problemsAccumulator: CollectionAccumulator[FhirMappingResult]): Unit

  /**
   * Validates the current FHIR writer. This method should be implemented by concrete subclasses to perform any necessary validation checks.
   * If the validation fails, an exception should be thrown.
   */
  def validate(): Unit
}

/**
 * Factory for FHIR writers
 */
object FhirWriterFactory {
  def apply(sinkSettings: FhirSinkSettings): BaseFhirWriter = {
    sinkSettings match {
      case frs: FhirRepositorySinkSettings => new FhirRepositoryWriter(frs)
      case fsss: FileSystemSinkSettings => new FileSystemWriter(fsss)
      case _ => throw new NotImplementedError()
    }
  }
}
