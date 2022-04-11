package io.onfhir.tofhir.engine

import io.onfhir.tofhir.model.{FhirRepositorySinkSettings}
import org.apache.spark.sql.{DataFrame}

/**
 * Class to write the dataset to given FHIR repository
 * @param sinkSettings                  Settings for the FHIR repository
 */
class FhirRepositoryWriter(sinkSettings:FhirRepositorySinkSettings) extends BaseFhirWriter(sinkSettings) {
  /**
   * Write the data frame of J to given FHIR repository
   *
   * @param df
   */
  override def write(df:DataFrame): Unit = ???
}
