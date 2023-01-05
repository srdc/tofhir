package io.tofhir.server.model

/**
 * GET all available schemas --> return a list of SchemaDefinitions where only url, type and name are populated.
 * GET a specific schema definition --> return a SchemaDefinition with all fields populated.
 * POST/CREATE a schema definition --> expect all required fields populated.
 * PUT/UPDATE a schema definition --> expect all required fields populated.
 */
case class SchemaDefinition(url: String, `type`: String, name: Option[String], rootDefinition: Option[SimpleStructureDefinition], fieldDefinitions: Option[Seq[SimpleStructureDefinition]])
