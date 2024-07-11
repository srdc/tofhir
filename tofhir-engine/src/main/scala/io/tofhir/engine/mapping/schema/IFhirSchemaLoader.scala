package io.tofhir.engine.mapping.schema

import org.apache.spark.sql.types.StructType

/**
 * Interface for mapping source schema loader
 */
trait IFhirSchemaLoader {
  /**
   * Read the schema given with the url and convert it to the Spark schema
   *
   * @param schemaUrl URL of the schema
   * @return
   */
  def getSchema(schemaUrl: String): Option[StructType]
}
