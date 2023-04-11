package io.tofhir.server.util

import io.tofhir.server.model.ResourceFilter
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, rand}

object DataFrameUtil {
  /**
   * Apply the row selection to the dataframe
   * @param df dataframe to select rows from
   * @param resourceFilter filter to apply
   * @return dataframe with selected rows
   */
  def applyResourceFilter(df: DataFrame, resourceFilter: ResourceFilter): DataFrame = {
    resourceFilter.order match {
      case "start" => df.limit(resourceFilter.numberOfRows)
      case "end" => df.withColumn("index", monotonically_increasing_id())
        .orderBy(col("index").desc)
        .drop("index")
        .limit(resourceFilter.numberOfRows)
      case "random" => df.orderBy(rand()).limit(resourceFilter.numberOfRows)
    }
  }

}
