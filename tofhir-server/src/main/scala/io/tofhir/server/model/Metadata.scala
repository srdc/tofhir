package io.tofhir.server.model

/**
 * Model that represents the metadata of the toFHIR server.
 *
 * @param name                    The name of the server.
 * @param description             A description of the server.
 * @param version                 The current version of the server.
 * @param fhirDefinitionsVersion  The major FHIR version of definitions (profiles, valuesets, codesystems) supported by the server.
 * @param toFhirRedcapVersion     The optional toFHIR-Redcap server version.
 * @param definitionsRootUrls     Optional list of root URLs for definitions.
 * @param schemasFhirVersion      The FHIR version used for schemas.
 * @param repositoryNames         The configured repository names.
 * @param archiving               The archiving configuration.
 * @param executionConfigurations The list of configurations used during the execution of mappings.
 */
case class Metadata(name: String,
                    description: String,
                    version: String,
                    fhirDefinitionsVersion: String,
                    toFhirRedcapVersion: Option[String],
                    definitionsRootUrls: Option[Seq[String]],
                    schemasFhirVersion: String,
                    repositoryNames: RepositoryNames,
                    archiving: Archiving,
                    environmentVariables: Map[String, String],
                    executionConfigurations: Seq[MappingExecutionConfiguration]
                   )

/**
 * Represents the configured repository names included in the metadata.
 *
 * @param mappings           The path to the folder where the mappings are kept.
 * @param schemas            The path to the folder where the schema definitions are kept.
 * @param contexts           The path to the folder where the mapping context files are kept.
 * @param jobs               The path to the folder where the job definitions are kept.
 * @param terminologySystems The path to the folder where the terminology system definitions are kept.
 */
case class RepositoryNames(mappings: String,
                           schemas: String,
                           contexts: String,
                           jobs: String,
                           terminologySystems: String)

/**
 * Archiving configs included inside the metadata.
 *
 * @param erroneousRecordsFolder   The parent folder where the data sources of errors received while running mapping are stored.
 * @param archiveFolder            The folder path where the archive of the processed source data is stored.
 * @param streamArchivingFrequency The period (in milliseconds) to run the archiving task for file streaming jobs.
 */
case class Archiving(erroneousRecordsFolder: String,
                     archiveFolder: String,
                     streamArchivingFrequency: Int)

/**
 * Represents a configuration used during the execution of mappings.
 *
 * @param name        The name of the configuration.
 * @param description A brief description of the configuration, explaining its purpose or usage.
 * @param value       The value assigned to the configuration, specifying its current setting.
 */
case class MappingExecutionConfiguration(name: String,
                                         description: String,
                                         value: String)
