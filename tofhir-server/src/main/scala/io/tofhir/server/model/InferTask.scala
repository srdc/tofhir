package io.tofhir.server.model

import io.tofhir.common.model.SimpleStructureDefinition
import io.tofhir.engine.model.{MappingJobSourceSettings, MappingSourceBinding}

/**
 * Infer task instance for getting source settings and source binding settings.
 * InferTask object is implemented only for SQL sources and file system sources.
 * @param name alias for source binding
 * @param mappingJobSourceSettings connection details for data source (e.g. JDBC connection details, file path)
 * @param sourceBinding mapping task source configuration (e.g. file name, table name, query)
 * @param inferenceType specifies how inferred fields should be applied when updating schemas:
 *                  - `"append"`: Infer the schema by preserving existing fields
 *                  - `"overwrite"`: Remove existing schema fields and return inferred fields.
 * @param fieldDefinitions an optional sequence of field definitions that represent the existing schema fields
 */
case class InferTask(name: String, mappingJobSourceSettings: Map[String, MappingJobSourceSettings], sourceBinding: MappingSourceBinding, inferenceType: String, fieldDefinitions: Option[Seq[SimpleStructureDefinition]])

/**
 * Enumeration of possible inference types.
 */
object InferenceTypes {
  val Append = "append"
  val OverWrite = "overwrite"
}