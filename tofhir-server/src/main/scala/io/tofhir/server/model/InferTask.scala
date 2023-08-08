package io.tofhir.server.model

import io.tofhir.engine.model.{DataSourceSettings, FhirMappingSourceContext}

/**
 * Infer task instance for getting connection settings and sql query. InferTask object is implemented only for SQL sources.
 *
 * @param sourceSettings Connection details for data source
 * @param fhirMappingSourceContext mapping task source information
 */
case class InferTask(sourceSettings: Map[String, DataSourceSettings], fhirMappingSourceContext: FhirMappingSourceContext)
