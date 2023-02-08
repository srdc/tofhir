package io.tofhir.server.model

/**
 * GET all available schemas --> return a list of SchemaDefinitions where only url, type and name are populated.
 * GET a specific schema definition --> return a SchemaDefinition with all fields populated.
 * POST/CREATE a schema definition --> expect all required fields populated.
 * PUT/UPDATE a schema definition --> expect all required fields populated.
 */

/**
 *
 * @param id      Identifier of the schema
 * @param url     URL of the schema
 * @param `type`  Type of entities that this schema represents
 * @param name    Name of the schema
 * @param project Identifier of the schema that this schema is associated with
 * @param rootDefinition
 * @param fieldDefinitions
 */
case class SchemaDefinition(id: String,
                            url: String,
                            `type`: String,
                            name: String,
                            project: String,
                            rootDefinition: Option[SimpleStructureDefinition],
                            fieldDefinitions: Option[Seq[SimpleStructureDefinition]])
