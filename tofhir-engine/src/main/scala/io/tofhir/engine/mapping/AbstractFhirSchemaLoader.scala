package io.tofhir.engine.mapping

import io.onfhir.api.Resource
import org.apache.spark.sql.types.StructType

/**
 * Base class to convert mapping source schemas given in FHIR StructureDefinitions to Spark schema
 *
 * @param majorFhirVersion FHIR version to be used to initialize the underlying FHIR parser.
 */
abstract class AbstractFhirSchemaLoader(majorFhirVersion: String = "R4") extends IFhirSchemaLoader {
  /**
   * Load the schema from the url and return parsed JSON
   *
   * @param url
   * @return
   */
  def loadSchema(url: String): Option[Resource]

  /**
   * Retrieves the schema with the given URL and transforms it to Spark's [[StructType]]
   *
   * @param schemaUrl URL of the schema
   * @return
   */
  def getSchema(schemaUrl: String): Option[StructType] = {
    loadSchema(schemaUrl).map(schemaInJson => new SchemaConverter(majorFhirVersion).convertSchema(schemaInJson))
  }
}
