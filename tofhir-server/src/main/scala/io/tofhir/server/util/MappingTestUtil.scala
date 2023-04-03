package io.tofhir.server.util

import io.tofhir.server.model.RowSelection
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, rand}

object MappingTestUtil {
  /**
   * Apply the row selection to the dataframe
   * @param df dataframe to select rows from
   * @param rowSelection row selection to apply
   * @return dataframe with selected rows
   */
  def selectRows(df: DataFrame, rowSelection: RowSelection): DataFrame = {
    rowSelection.order match {
      case "start" => df.limit(rowSelection.numberOfRows)
      case "end" => df.withColumn("index", monotonically_increasing_id())
        .orderBy(col("index").desc)
        .drop("index")
        .limit(rowSelection.numberOfRows)
      case "random" => df.orderBy(rand()).limit(rowSelection.numberOfRows)
    }
  }

}
