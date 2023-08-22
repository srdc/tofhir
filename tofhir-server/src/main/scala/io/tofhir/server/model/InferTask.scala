package io.tofhir.server.model

import io.tofhir.engine.model.{DataSourceSettings, FhirMappingSourceContext}

/**
 * Infer task instance for getting source settings and source context settings.
 * InferTask object is implemented only for SQL sources and file system sources.
 * @param name alias for source context
 * @param sourceSettings connection details for data source (e.g. JDBC connection details, file path)
 * @param sourceContext mapping task source context (e.g. file name, table name, query)
 */
case class InferTask(name: String, sourceSettings: Map[String, DataSourceSettings], sourceContext: FhirMappingSourceContext)
