package io.onfhir.tofhir.data.write

import io.onfhir.tofhir.model.{FhirRepositorySinkSettings, FhirSinkSettings}
import org.apache.spark.sql.Dataset

/**
 * Base class for FHIR resource writer
 *
 * @param sinkSettings
 * @tparam S
 */
abstract class BaseFhirWriter(sinkSettings: FhirSinkSettings) extends Serializable {
  /**
   * Write the data frame of json serialized FHIR resources to given sink (e.g. FHIR repository)
   *
   * @param df
   */
  def write(df: Dataset[String]): Unit
}

/**
 * Factory for FHIR writers
 */
object FhirWriterFactory {
  def apply(sinkSettings: FhirSinkSettings): BaseFhirWriter = {
    sinkSettings match {
      case frs: FhirRepositorySinkSettings => new FhirRepositoryWriter(frs)
      case _ => throw new NotImplementedError()
    }
  }
}
