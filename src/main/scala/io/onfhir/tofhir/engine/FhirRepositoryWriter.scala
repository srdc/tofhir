package io.onfhir.tofhir.engine

import io.onfhir.tofhir.model.{FhirRepositorySinkSettings, MappedFhirResource}
import org.apache.spark.sql.Dataset

/**
 * Class to write the dataset to given FHIR repository
 * @param sinkSettings                  Settings for the FHIR repository
 * @tparam FhirRepositorySinkSettings
 */
class FhirRepositoryWriter(sinkSettings:FhirRepositorySinkSettings) extends BaseFhirWriter(sinkSettings) {
  /**
   * Write the data frame of J to given FHIR repository
   *
   * @param df
   */
  override def write(df:Dataset[MappedFhirResource]): Unit = ???
}
