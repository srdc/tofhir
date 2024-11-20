package io.tofhir.server.util

import io.tofhir.server.model.{ResourceFilter, RowSelectionOrder}
import org.apache.spark.sql.DataFrame

object DataFrameUtil {
  /**
   * Apply the row selection to the dataframe
   *
   * @param df             dataframe to select rows from
   * @param resourceFilter filter to apply
   * @return dataframe with selected rows
   */
  def applyResourceFilter(df: DataFrame, resourceFilter: ResourceFilter): DataFrame = {
    resourceFilter.order match {
      case RowSelectionOrder.START =>
        df.limit(resourceFilter.numberOfRows)
      case RowSelectionOrder.RANDOM =>
        // Using a sampling fraction to reduce the cost of ordering
        // sampleFraction cannot be higher than 1.0
        val sampleFraction = 1.0.min(resourceFilter.numberOfRows.toDouble / df.count())
        df.sample(sampleFraction).limit(resourceFilter.numberOfRows)
    }
  }

}
