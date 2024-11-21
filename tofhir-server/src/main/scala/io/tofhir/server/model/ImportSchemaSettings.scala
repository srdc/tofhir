package io.tofhir.server.model

import io.onfhir.client.model.IFhirRepositorySecuritySettings

/**
 * Represents the settings required for importing a schema from a FHIR server.
 *
 * @param baseUrl The base URL of the FHIR server from which to import the schema.
 * @param resourceId The ID of the resource representing the schema i.e. StructureDefinition to be imported.
 * @param securitySettings Optional security settings for accessing the FHIR server, if applicable.
 */
case class ImportSchemaSettings(baseUrl: String, resourceId: String, securitySettings: Option[IFhirRepositorySecuritySettings])