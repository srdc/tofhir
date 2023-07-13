package io.tofhir.server.model

import io.tofhir.engine.model.{DataSourceSettings, SqlSource}

/**
 * Infer task instance for getting connection settings and sql query. InferTask object is implemented only for SQL sources.
 *
 * @param sourceSettings    Connection details for data source
 * @param sqlSource         Sql query for data source
 */
case class InferTask(sourceSettings: Map[String, DataSourceSettings], sqlSource: SqlSource)
