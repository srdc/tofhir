package io.tofhir.server.model

import java.util.UUID

/**
 * Local Terminology Service
 *
 * @param id unique id
 * @param name name of the terminology
 * @param description description of the terminology
 * @param folderPath folder path of the terminology
 */
case class LocalTerminology(id: String = UUID.randomUUID().toString,
                            name: String,
                            description: String,
                            folderPath: String,
                            conceptMaps: Seq[TerminologyConceptMap] = Seq.empty,
                            codeSystems: Seq[TerminologyCodeSystem] = Seq.empty) {
  def withId(id: String): LocalTerminology = {
    this.copy(id = id)
  }
}

/**
 * Local Terminology ConceptMap
 * @param id unique id
 * @param fileName name of the terminology
 * @param conceptMapUrl concept map url of the terminology
 * @param sourceValueSetUrl source value set url of the terminology
 * @param targetValueSetUrl target value set url of the terminology
 */
case class TerminologyConceptMap(id: String = UUID.randomUUID().toString, fileName: String, conceptMapUrl: String, sourceValueSetUrl: String, targetValueSetUrl: String)

/**
 * Local Terminology CodeSystem
 * @param id unique id
 * @param fileName file name of the terminology
 * @param codeSystem code system of the terminology
 */
case class TerminologyCodeSystem(id: String = UUID.randomUUID().toString, fileName: String, codeSystem: String)
