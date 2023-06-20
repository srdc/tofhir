package io.tofhir.server.model

import io.tofhir.engine.model.{DataSourceSettings, SqlSource}
/**
 * Infer task instance
 *
 * @param sourceSettings    Connection details for data source
 * @param sqlSource         Sql query for data source
 */
case class InferTask(sourceSettings: Map[String, DataSourceSettings], sqlSource: SqlSource)
