package io.tofhir.server.model

import io.tofhir.engine.model.{MappingJobSourceSettings, MappingSourceBinding}

/**
 * Infer task instance for getting source settings and source binding settings.
 * InferTask object is implemented only for SQL sources and file system sources.
 * @param name alias for source binding
 * @param mappingJobSourceSettings connection details for data source (e.g. JDBC connection details, file path)
 * @param sourceBinding mapping task source configuration (e.g. file name, table name, query)
 */
case class InferTask(name: String, mappingJobSourceSettings: Map[String, MappingJobSourceSettings], sourceBinding: MappingSourceBinding)
