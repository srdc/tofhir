package io.tofhir.server.model

/**
 * Entity representing a FHIR StructureDefinition in the context of toFHIR
 *
 * @param id               Identifier of the schema
 * @param url              URL of the schema
 * @param `type`           Type of entities that this schema represents
 * @param name             Name of the schema
 * @param rootDefinition   Root element definition for the schema i.e. the first element in the definition
 * @param fieldDefinitions Rest of the element definitions
 */
case class SchemaDefinition(id: String,
                            url: String,
                            `type`: String,
                            name: String,
                            rootDefinition: Option[SimpleStructureDefinition],
                            fieldDefinitions: Option[Seq[SimpleStructureDefinition]])
