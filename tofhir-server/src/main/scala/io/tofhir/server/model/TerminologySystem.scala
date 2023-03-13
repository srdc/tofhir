package io.tofhir.server.model

import io.tofhir.engine.model.{CodeSystemFile, ConceptMapFile}
import io.tofhir.server.model.TerminologySystem.{TerminologyCodeSystem, TerminologyConceptMap}

import java.util.UUID

/**
 * Local Terminology System
 *
 * @param id          unique id
 * @param name        name of the terminology
 * @param description description of the terminology
 */
case class TerminologySystem(id: String = UUID.randomUUID().toString,
                             name: String,
                             description: String,
                             conceptMaps: Seq[TerminologyConceptMap] = Seq.empty,
                             codeSystems: Seq[TerminologyCodeSystem] = Seq.empty)

object TerminologySystem {
  type TerminologyConceptMap = ConceptMapFile
  type TerminologyCodeSystem = CodeSystemFile
}

//
///**
// * Local Terminology ConceptMap
// *
// * @param id                unique id
// * @param name              name of the terminology concept map
// * @param conceptMapUrl     url of the terminology concept map
// * @param sourceValueSetUrl source value set url of the terminology concept map
// * @param targetValueSetUrl target value set url of the terminology concept map
// */
//case class TerminologyConceptMap(id: String = UUID.randomUUID().toString, override val name: String, override val conceptMapUrl: String, override val sourceValueSetUrl: String,  override val targetValueSetUrl: String)
//  extends ConceptMapFile(name: String, conceptMapUrl: String, sourceValueSetUrl: String, targetValueSetUrl: String)
//
///**
// * Local Terminology CodeSystem
// *
// * @param id         unique id
// * @param name       name of the terminology code system
// * @param codeSystem code system of the terminology code system
// */
//case class TerminologyCodeSystem(id: String = UUID.randomUUID().toString, name: String, codeSystem: String)
