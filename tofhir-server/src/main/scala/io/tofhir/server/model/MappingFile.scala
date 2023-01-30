package io.tofhir.server.model

/**
 * Represents a mapping file
 * @param name Name of the mapping file
 * @param folder the sub-folder it is in
 */
case class MappingFile(name: String, folder: String)
