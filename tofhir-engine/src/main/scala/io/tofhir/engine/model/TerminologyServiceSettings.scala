package io.tofhir.engine.model

import java.util.UUID

/**
 * Interface for settings for terminology service
 */
trait TerminologyServiceSettings


/**
 * A local terminology service where each concept map is provided via a CSV file
 *
 * @param folderPath      Path to the folder that all concept map files exists
 * @param conceptMapFiles List of concept map files
 */
case class LocalFhirTerminologyServiceSettings(folderPath: String, conceptMapFiles: Seq[ConceptMapFile] = Nil, codeSystemFiles: Seq[CodeSystemFile] = Nil) extends TerminologyServiceSettings

/**
 * Metadata for a Concept map file for toFhir local terminology service
 *
 * @param id                unique id
 * @param name              Name of the file
 * @param conceptMapUrl     Corresponding concept map url e.g. http://cds-hooks.hl7.org/ConceptMap/indicator-to-request-priority
 * @param sourceValueSetUrl Url for Source value set
 * @param targetValueSetUrl Url for Target value set
 */
case class ConceptMapFile(id: String = UUID.randomUUID().toString, name: String, conceptMapUrl: String, sourceValueSetUrl: String, targetValueSetUrl: String)

/**
 * Metadata for a code system file  for toFhir local terminology service
 *
 * @param id         unique id
 * @param name       Name of the file
 * @param codeSystem Corresponding code system url
 */
case class CodeSystemFile(id: String = UUID.randomUUID().toString, name: String, codeSystem: String)
