package io.onfhir.tofhir.engine


import io.onfhir.tofhir.model.{FhirRepositorySinkSettings, FhirSinkSettings, MappedFhirResource}
import org.apache.spark.sql.{DataFrame, Dataset}

/**
 * Base class for FHIR resource writer
 * @param sinkSettings
 * @tparam S
 */
abstract class BaseFhirWriter(sinkSettings:FhirSinkSettings) {
  /**
   * Write the data frame to given sink (e.g. FHIR repository)
   * @param df
   */
  def write(df:DataFrame):Unit
}

/**
 * Factory for FHIR writers
 */
object FhirWriterFactory {
  def apply(sinkSettings:FhirSinkSettings):BaseFhirWriter = {
    sinkSettings match {
      case frs :FhirRepositorySinkSettings => new FhirRepositoryWriter(frs)
      case _ => throw new NotImplementedError()
    }
  }
}