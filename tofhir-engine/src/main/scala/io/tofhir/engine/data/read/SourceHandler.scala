package io.tofhir.engine.data.read

import io.tofhir.engine.model.exception.FhirMappingException
import io.tofhir.engine.model.{DataSourceSettings, FhirMappingSourceContext}
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

object SourceHandler {
  //Column name to append to the source data frame, to indicate whether input is valid or not
  final val INPUT_VALIDITY_ERROR = "__validationError"

  /**
   * Reading data from an input source
   *
   * @param alias          Name of the source
   * @param spark          Spark session
   * @param mappingSource  Source definition e.g. See FileSystemSource
   * @param sourceSettings General source settings e.g. See FileSystemSourceSettings
   * @param schema         Schema of the input supplied by the mapping definition
   * @param timeRange      Time range for the data to read if given
   * @param limit          Limit the number of rows to read
   * @param jobId          The identifier of mapping job which executes the mapping
   * @tparam T Type of the source definition class
   * @tparam S Type of the source settings class
   * @return
   */
  def readSource[T <: FhirMappingSourceContext, S <: DataSourceSettings](
                                                                          alias: String,
                                                                          spark: SparkSession,
                                                                          mappingSource: T,
                                                                          sourceSettings: S,
                                                                          schema: Option[StructType],
                                                                          timeRange: Option[(LocalDateTime, LocalDateTime)] = Option.empty,
                                                                          limit: Option[Int] = Option.empty,
                                                                          jobId: Option[String] = Option.empty
                                                                        ): DataFrame = {
    val reader = try {
      DataSourceReaderFactory
        .apply(spark, mappingSource, sourceSettings)
    }
    catch {
      case e: Throwable => throw FhirMappingException(s"Failed to construct reader for mapping source: $mappingSource source settings: $sourceSettings.", e)
    }

    val sourceData = try {
      reader
        .read(mappingSource, sourceSettings, schema, timeRange, limit, jobId = jobId)
    } catch {
      case e: Throwable => throw FhirMappingException(s"Source cannot be read for mapping source: $mappingSource source settings: $sourceSettings.", e)
    }

    val finalSourceData = try {
      //If there is some preprocessing SQL defined, apply it
      mappingSource.preprocessSql match {
        case Some(sql) =>
          sourceData.createOrReplaceTempView(alias)
          spark.sql(sql)
        case None =>
          sourceData
      }
    } catch {
      case e: Throwable => throw FhirMappingException(s"Erroneous Preprocess SQL: ${mappingSource.preprocessSql}", e)
    }
    schema match {
      //If there is a schema and also need validation
      case Some(sc) if reader.needTypeValidation || reader.needCardinalityValidation =>
        //TODO handle type validation

        // Find the required fields
        // Create a mutable set to store the names of required fields.
        var requiredFields = sc.fields.filterNot(_.nullable).map(_.name).toSet
        // Filter the set of required fields to ensure that their parents are also required
        requiredFields = requiredFields.filter(field => {
          // Split the field name into parts using dot (.) as the separator.
          val parts = field.split("\\.")
          // Check if each part's parent is also required.
          parts.indices.forall { i =>
            // Create the parent name by joining the parts up to index 'i' with dots.
            val parentName = parts.take(i + 1).mkString(".")
            // Check if the parent is required.
            requiredFields.contains(parentName)
          }
        })
        if (requiredFields.isEmpty)
          finalSourceData.withColumn(INPUT_VALIDITY_ERROR, lit(null).cast(DataTypes.StringType))
        else {
          //TODO handle required fields for non-tabular data (deep fields)
          //Check required columns
          val nullCheck =
            requiredFields
              .map(f => col(f).isNull)
              .reduce((c1, c2) => c1 || c2)

          finalSourceData
            .withColumn(INPUT_VALIDITY_ERROR,
              when(nullCheck, lit(s"One of the required column(s) (${requiredFields.mkString(", ")}) is missing or null"))
                .otherwise(lit(null).cast(DataTypes.StringType))
            )
        }
      //If there is no schema or readers don't need validation, we assume all rows are valid
      case _ =>
        finalSourceData.withColumn(INPUT_VALIDITY_ERROR, lit(null).cast(DataTypes.StringType))
    }
  }
}
