package io.tofhir.server.model

/**
 * FhirMappingMetadata model in the projects.json file
 * We dont want fields in FhirMapping (context, variable, mapping) in the projects.json file.
 * If we change those fields to Option, many in the engine codebase will be effected. So, new class is created
 * @param id Identifier of the mapping
 * @param url URL of the mapping
 * @param name Name of the mapping file
 */
case class FhirMappingMetadata(id: String, url: String, name: String)
