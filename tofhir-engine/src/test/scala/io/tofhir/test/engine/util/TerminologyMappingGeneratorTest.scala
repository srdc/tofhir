package io.tofhir.test.engine.util

import io.tofhir.engine.util.mappinggenerator.{GeneratorDBAdapter, OmopConcept, OmopConceptRelationship, TerminologyMappingGenerator, TerminologySystemMapping}
import org.mockito.MockitoSugar._
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TerminologyMappingGeneratorTest extends AnyFlatSpec with Matchers with PrivateMethodTester {

  val mockDbAdapter: GeneratorDBAdapter = mock[GeneratorDBAdapter]
  when(mockDbAdapter.getConcepts("SNOMED", "Some domain")).thenReturn(Set(
    OmopConcept(1, "SNOMED", "SNOMED-1", "SNOMED-1"),
    OmopConcept(2, "SNOMED", "SNOMED-2", "SNOMED-2"),
    OmopConcept(3, "SNOMED", "SNOMED-3", "SNOMED-3")
  ))
  when(mockDbAdapter.getConcepts("ICD10", "Some domain")).thenReturn(Set(
    OmopConcept(4, "ICD10", "ICD10-1", "ICD10-1")
  ))
  when(mockDbAdapter.getOmopConceptRelationships(Set("Mapped from"))).thenReturn(Set(
    OmopConceptRelationship(1, 2, "Mapped from"),
    OmopConceptRelationship(2, 4, "Mapped from"),
    OmopConceptRelationship(3, 4, "Mapped from")
  ))

  val generator: TerminologyMappingGenerator = new TerminologyMappingGenerator(mockDbAdapter)

  "TerminologySystemMappingGenerator" should "construct concepts and relationships" in {
    val populateConceptsAndRelationships = PrivateMethod[Set[TerminologySystemMapping]](Symbol("populateConceptsAndRelationships"))
    generator invokePrivate populateConceptsAndRelationships("SNOMED", "ICD10", "Some domain", Set("Mapped from"))
    generator.concepts.size shouldEqual 4
    generator.relationships.size shouldEqual 3
  }

  "TerminologySystemMappingGenerator" should "extract implied mappings" in {
    val extractMappings = PrivateMethod[Set[TerminologySystemMapping]](Symbol("extractMappings"))
    val mappings = generator invokePrivate extractMappings("SNOMED", "ICD10", "SomeUrl", "Some domain")
    mappings.size shouldEqual 3
    mappings.exists(mapping => mapping.source_code.equals("1") && mapping.target_code.equals("ICD10-1")) shouldBe true
    mappings.exists(mapping => mapping.source_code.equals("2") && mapping.target_code.equals("ICD10-1")) shouldBe true
    mappings.exists(mapping => mapping.source_code.equals("3") && mapping.target_code.equals("ICD10-1")) shouldBe true
  }

}
