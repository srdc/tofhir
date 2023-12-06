package io.tofhir.engine.util.mappinggenerator

import scala.beans.BeanProperty

case class OmopConcept(concept_id: Int, vocabulary_id: String, concept_code: String, concept_name: String)

case class OmopConceptRelationship(concept_id_1: Int, concept_id_2: Int, relationship_id: String)

case class TerminologySystemMapping(@BeanProperty source_system: String,
                                    @BeanProperty source_code: String,
                                    @BeanProperty target_system: String,
                                    @BeanProperty target_code: String,
                                    @BeanProperty target_display: String)
