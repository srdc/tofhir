package io.tofhir.server.model

/**
 * Model that represents the metadata of the server.
 */
case class Metadata(name: String,
                    description: String,
                    version: String,
                    majorFhirVersion: String,
                    toFhirRedcapVersion: Option[String],
                    definitionsRootUrls: Option[Seq[String]],
                    schemasFhirVersion: String,
                    repositoryNames: RepositoryNames,
                    archiving: Archiving
                   )

/**
 * Configured repository names model to return inside the metadata.
 */
case class RepositoryNames(mappings: String,
                           schemas: String,
                           contexts: String,
                           jobs: String,
                           terminologySystems: String)

/**
 * Archiving configs included inside the metadata.
 */
case class Archiving(erroneousRecordsFolder: String,
                     archiveFolder: String,
                     streamArchivingFrequency: Int)
