package io.tofhir.server.model

/**
 * Represents a mapping file
 * @param id Identifier of the mapping
 * @param url URL of the mapping
 * @param name Name of the mapping file
 */
case class MappingMetadata(id: String, url: String, name: String)
