package io.ignifyr.engine.model

/**
 * FHIR Mapping task instance
 * {@link mapping} will be executed if it is provided. Otherwise, the mapping referenced by {@link mappingRef} is
 * retrieved from the repository and executed.
 *
 * @param name              Name of the given FhirMappingTask
 * @param mappingRef        Canonical URL of the FhirMapping definition to execute
 * @param sourceBinding     A map that provides details on how to load each source data for the mapping.
 *                          It links the source settings of a mapping job to the sources of a mapping.
 * @param mapping           FhirMapping definition to execute
 * @param batchingStrategy  Optional batching strategy to process data in multiple batches based on custom parameters
 *                          (e.g., by year, ID range, or any custom grouping). If provided, the mapping will be executed
 *                          once for each parameter value, with the parameter available in preprocessSql as $parameterName
 */
case class FhirMappingTask(
    name: String,
    mappingRef: String,
    sourceBinding: Map[String, MappingSourceBinding],
    mapping: Option[FhirMapping] = None,
    batchingStrategy: Option[BatchingStrategy] = None
) {

  /**
   * Substitutes batch parameters in the preprocessSql of all source bindings of this mapping task.
   * Parameters in preprocessSql are specified as $parameterName and will be replaced with the corresponding value.
   *
   * @param parameters Map of parameter names to their values (e.g., Map("year" -> "2014", "month" -> "1"))
   * @return A new mapping task with substituted preprocessSql in all source bindings
   */
  def substituteBatchParameters(parameters: Map[String, String]): FhirMappingTask = {
    // Longest parameter name first. Substituting a shorter name first would rewrite the prefix of a
    // longer one that starts with it: with `year` and `yearEnd` both defined, `$yearEnd` would become
    // `2020End`. Map iteration order is unspecified, so relying on it is not an option either.
    val orderedParameters = parameters.toSeq.sortBy(-_._1.length)
    val updatedSourceBinding = sourceBinding.map { case (alias, binding) =>
      alias -> (binding.preprocessSql match {
        case Some(sql) =>
          val substitutedSql = orderedParameters.foldLeft(sql) { case (currentSql, (paramName, paramValue)) =>
            currentSql.replace(s"$$$paramName", paramValue)
          }
          binding.withPreprocessSql(Some(substitutedSql))
        case None => binding
      })
    }
    copy(sourceBinding = updatedSourceBinding)
  }
}

/**
 * Interface for defining the mapping source binding in a mapping job.
 *
 * This interface is used to provide a way to link source settings of a mapping job
 * to the corresponding sources in a mapping. Implementations of this interface
 * should specify how to load each source data required by the mapping.
 */
trait MappingSourceBinding extends Serializable {

  /**
   * SQL Query to preprocess the source data
   */
  val preprocessSql: Option[String] = None

  /**
   * Reference to the source specified in the sourceSettings of a mapping job.
   * This optional field can be used to explicitly link a source binding to a particular source setting.
   * If not provided, the source alias from the mapping will be used to determine the source settings,
   * ensuring backward compatibility.
   */
  val sourceRef: Option[String] = None

  /**
   * A copy of this binding with the given preprocess SQL, so callers can rewrite the query
   * (e.g. batch-parameter substitution) without matching on concrete binding types.
   */
  def withPreprocessSql(preprocessSql: Option[String]): MappingSourceBinding

  /**
   * A copy of this binding with the `${ENV_VAR}` placeholders of its fields resolved by `resolve` (see
   * `EnvironmentVariableResolver`). Each binding type decides which of its fields may carry one; the default is
   * none.
   */
  def withResolvedEnvironmentVariables(resolve: String => String): MappingSourceBinding = this
}

/**
 * Context/configuration for one of the sources of the mapping that will read the source data from the file system.
 *
 * For batch jobs, you should provide the folder name in the "path" field and indicate the content type in the "contentType"
 * field. It reads the files with the specified content type in the given folder.
 *
 * Example for Batch Jobs:
 *   - With extension in path:
 *     FileSystemSource(path = "data/patients.csv") => Will read the "data/patients.csv" file.
 *   - Providing folder name and content type:
 *     FileSystemSource(path = "data/patients", contentType = "csv") => Will read all files in the "data/patients" folder and interpret them as a csv.
 *
 * For streaming jobs, you should provide a folder name in the "path" field and a content type in the "contentType" field so that
 * it can read the files with the specified content type in the given folder.
 *
 * Examples for Streaming Jobs:
 *   - Providing folder name and content type:
 *     FileSystemSource(path = "data/streaming/patients", contentType = "json") => Will read all files in the "data/streaming/patients" folder in json content type.
 * @param path        File path to the source file or folder, e.g., "patients.csv" or "patients".
 * @param contentType Content of the file
 * @param options     Further options for the content type (Spark Data source options for the content type, e.g., for csv -> https://spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option).
 */
case class FileSystemSource(
    path: String,
    contentType: String,
    options: Map[String, String] = Map.empty[String, String],
    override val preprocessSql: Option[String] = None,
    override val sourceRef: Option[String] = None
) extends MappingSourceBinding {
  override def withPreprocessSql(preprocessSql: Option[String]): MappingSourceBinding =
    copy(preprocessSql = preprocessSql)
}

/**
 * Context/configuration for one of the source of the mapping that will read the source data from an SQL database
 * Any of tableName and query must be defined. Not both, not neither
 *
 * @param tableName Name of the table
 * @param query     Query to execute in the database
 * @param options   Further options for SQL source (Spark SQL Guide -> https://spark.apache.org/docs/3.4.1/sql-data-sources-jdbc.html ).
 */
case class SqlSource(
    tableName: Option[String] = None,
    query: Option[String] = None,
    options: Map[String, String] = Map.empty[String, String],
    override val preprocessSql: Option[String] = None,
    override val sourceRef: Option[String] = None
) extends MappingSourceBinding {
  override def withPreprocessSql(preprocessSql: Option[String]): MappingSourceBinding =
    copy(preprocessSql = preprocessSql)
}

/**
 * List of source content types supported by ignifyr
 */
object SourceContentTypes {
  final val CSV = "csv"
  final val TSV = "tsv"
  final val PARQUET = "parquet"
  final val JSON = "json"
  final val NDJSON = "ndjson"
}
