package io.tofhir.engine.util.mappinggenerator

import com.fasterxml.jackson.dataformat.csv.{CsvMapper, CsvSchema}
import io.tofhir.engine.mapping.service.LocalTerminologyService.ConceptMapFileColumns
import io.tofhir.engine.util.mappinggenerator.TerminologyMappingGenerator._

import java.io.File
import scala.collection.mutable
import scala.jdk.CollectionConverters.IterableHasAsJava

object TerminologyMappingGenerator {

  private val OMOP_URL: String = "https://www.ohdsi.org/omop"

  /**
   * Example usage
   */
  def main(args: Array[String]): Unit = {
    new TerminologyMappingGenerator(new GeneratorDBAdapter()).generateMappings("SNOMED", "ICD10","http://hl7.org/fhir/sid/icd-10", "Condition", Set("Mapped from"), "icd10cm.csv")
  }

}

/**
 * A class to generate file-based terminology mappings by parsing the relationships defined in the OMOP vocabulary.
 * The generator creates terminology mappings from a source terminology to a target one e.g. from SNOMED to ICD10.
 *
 * <b>A note on the semantics of the mappings:</b>
 * <p>
 *   The mappings captured by this generated cover all the equivalence relationships (formed by the 'Mapped from' relationship by default),
 *   which are not necessarily 1 to 1. For example, a single generic SNOMED code can map to thousands of ICD codes. This 1 to many
 *   characteristic of the mappings should be considered when the output of the generator is to be used as a terminology service.
 * </p>
 */
class TerminologyMappingGenerator(dbAdapter: GeneratorDBAdapter) {
  // All concepts defined in the OMOP vocabulary associated with the source and target terminologies.
  // It is a map from the OMOP concept id to the concept object including the details about the concept.
  var concepts: mutable.Map[Int, OmopConcept] = mutable.Map.empty
  // All relationships defined in the OMOP vocabulary. The relationships are represented as map of (OMOP concept id to set of OMOP concepts) entries where the
  // key represents the identifier of the source concept and set of concepts represent the concepts from the target terminology system equivalent to the source concept.
  var relationships: mutable.Map[Int, mutable.Set[OmopConcept]] = mutable.Map.empty

  /**
   * Generates a file including terminology translations from a source code system to a target code system.
   *
   * @param sourceSystem         OMOP vocabulary ID for the source terminology
   * @param targetSystem         OMOP vocabulary ID for the target terminology
   * @param targetSystemUrl      URL for the target vocabulary. It will be set as the URL of the target codes
   * @param conceptDomain        OMOP domain of the concepts of interest e.g. Condition, Procedure, Measurement, etc.
   * @param conceptRelationships Relationships types to be considered as defined in the OMOP vocabulary e.g. Mapped from
   * @param outputFile           Name of the output file
   */
  def generateMappings(sourceSystem: String, targetSystem: String, targetSystemUrl: String, conceptDomain: String, conceptRelationships: Set[String], outputFile: String): Unit = {
    // Retrieve all the concepts associated with the source and target code systems (terminologies) and all relationships are obtained from the OMOP vocabulary
    populateConceptsAndRelationships(sourceSystem, targetSystem, conceptDomain, conceptRelationships)

    // extract mappings by traversing the relationships
    val terminologySystemMappings = extractMappings(sourceSystem, targetSystem, targetSystemUrl, conceptDomain)

    // DB operations are complete, clear any resources
    dbAdapter.clear()

    // Generate the file using the accumulated mappings
    generateTerminologySystemFile(terminologySystemMappings, outputFile)
  }

  /**
   * This method traverses the direct or indirect relationships from the source system to the target system.
   * For each code defined in the source terminology, all mappings leading to a code from the target terminology are identified.
   * A CSV file with the given name is created containing the identified mappings. While the URL of the target terminology codes are specified explicitly,
   * the URL of the source terminology is set to [[OMOP_URL]] as the source codes are in fact OMOP concept identifiers representing a standard-based code.
   * (This does not have a practical implication concerning the mapping, so can be changed).
   *
   * The following columns are included in the CSV: "source_system","source_code","target_system","target_code","target_display"
   * An example row would be:
   * <ul>
   * <li>source system: www.ohdsi.org/omop -> Omop URL</li>
   * <li>source_code: 3003176 -> OMOP concept id for the LOINC code 11727-5</li>
   * <li>target_system: http://loinc.org -> LOINC terminology URL</li>
   * <li>target_code: 11727-5 -> LOINC code as the target code</li>
   * <li>target_display: Body weight</li>
   * </ul>
   *
   * @param sourceSystem
   * @param targetSystem
   * @param targetSystemUrl
   * @param conceptDomain
   * @return
   */
  private def extractMappings(sourceSystem: String, targetSystem: String, targetSystemUrl: String, conceptDomain: String): Set[TerminologySystemMapping] = {
    // Each source system concept is a starting point to initiate a mapping identification process
    val sourceSystemConcepts: Set[OmopConcept] = dbAdapter.getConcepts(sourceSystem, conceptDomain)
    // Visited concepts will keep the concepts that are already considered so that they won't be considered again. This case might happen
    // when a concept is considered already while identifiying mappings for an upstream concept. For example:
    // Assuming we have such a mapping structure: SNOMED code 1 -> SNOMED code 2 -> ICD, indicating that SNOMED code 1 maps to SNOMED code 2; and SNOMED code 2
    // maps to an ICD code. While getting the mappings for SNOMED code 1, we will have processed the SNOMED code 2 as well. Therefore, it won't need to be processed once again.
    val visitedConcepts: mutable.Set[OmopConcept] = mutable.Set.empty[OmopConcept]
    // Keeps the terminology system concepts to be outputted to the CSV file
    val terminologySystemMappings: mutable.Set[TerminologySystemMapping] = mutable.Set.empty[TerminologySystemMapping]

    // Find mappings for each source concept unless it's been processed already
    sourceSystemConcepts.toSeq.foreach(concept => {
      if (!visitedConcepts.contains(concept)) {
        val mappings: Set[TerminologySystemMapping] = findMappings(concept, targetSystem, targetSystemUrl, visitedConcepts)
        terminologySystemMappings.addAll(mappings)
        if (mappings.nonEmpty) {
          println(s"Retrieved ${mappings.size} mappings for concept: $concept")
        }
      }
    })
    terminologySystemMappings.toSet
  }

  /**
   * Writes the given [[TerminologySystemMapping]]s to the specified output file.
   *
   * @param terminologySystemConcepts Set of mappings. Each mapping will be serialized as a row in the CSV file.
   * @param outputFile                Name of the output file
   */
  private def generateTerminologySystemFile(terminologySystemConcepts: Set[TerminologySystemMapping], outputFile: String): Unit = {
    val myObjectWriter = new CsvMapper()
      .writerFor(classOf[TerminologySystemMapping])
      .`with`(getCsvSchema())
    val tempFile = new File(outputFile)
    myObjectWriter.writeValues(tempFile).writeAll(terminologySystemConcepts.asJava)
  }

  /**
   * Finds mappings for an individual source code (OMOP concept). Applies BFS to discover chains of relationships such as NOMED code 1 -> SNOMED code ->2 -> ICD.
   * In this example; although SNOMED code 1 does not have direct mapping to ICD code, it has a mapping via SNOMED code 2. The search continues until all
   * the relationships are processed. Codes belonging to the target terminology are accumulated and returned as [[TerminologySystemMapping]].
   *
   * @param sourceConcept   Source concept of the mapping
   * @param targetSystem    OMOP vocabulary id of the target terminology
   * @param targetSystemUrl URL for the target terminology
   * @param visitedConcepts A set containing the visited concepts during earlier operations
   * @return
   */
  private def findMappings(sourceConcept: OmopConcept, targetSystem: String, targetSystemUrl: String, visitedConcepts: mutable.Set[OmopConcept]): Set[TerminologySystemMapping] = {
    val visitQueue: mutable.Queue[OmopConcept] = mutable.Queue.empty[OmopConcept]
    val equivalentSourceConcepts: mutable.Set[OmopConcept] = mutable.Set.empty
    val equivalentTargetConcepts: mutable.Set[OmopConcept] = mutable.Set.empty

    visitQueue.enqueue(sourceConcept)
    while (visitQueue.nonEmpty) {
      val currentCode: OmopConcept = visitQueue.dequeue()
      equivalentSourceConcepts.add(currentCode)
      if (!visitedConcepts.contains(currentCode)) {
        visitedConcepts.add(currentCode)
        relationships.get(currentCode.concept_id) match {
          case None =>
          case Some(mappings) =>
            mappings.foreach(concept => {
              if (concept.vocabulary_id.equals(targetSystem)) {
                equivalentTargetConcepts.add(concept)
              } else {
                visitQueue.enqueue(concept)
              }
            })
        }
      }
    }
    equivalentSourceConcepts.flatMap(sourceConcept => {
      equivalentTargetConcepts.map(targetConcept => {
        TerminologySystemMapping(OMOP_URL, sourceConcept.concept_id.toString, targetSystemUrl, targetConcept.concept_code, targetConcept.concept_name)
      })
    }).toSet
  }

  /**
   * Obtains concepts for the source and target terminologies and relationships as defined in the OMOP vocabulary.
   *
   * @param sourceSystem
   * @param targetSystem
   * @param conceptDomain
   * @param conceptRelationships
   */
  private def populateConceptsAndRelationships(sourceSystem: String, targetSystem: String, conceptDomain: String, conceptRelationships: Set[String]): Unit = {
    dbAdapter.getConcepts(sourceSystem, conceptDomain).foreach(concept => concepts.put(concept.concept_id, concept))
    dbAdapter.getConcepts(targetSystem, conceptDomain).foreach(concept => concepts.put(concept.concept_id, concept))
    dbAdapter.getOmopConceptRelationships(conceptRelationships)
      .foreach(relationship => {
        val conceptMappings: mutable.Set[OmopConcept] = relationships.getOrElseUpdate(relationship.concept_id_1, mutable.Set.empty[OmopConcept])
        if (concepts.contains(relationship.concept_id_2)) {
          conceptMappings.add(concepts(relationship.concept_id_2))
        }
      })
  }

  /**
   * Creates a [[CsvSchema]] conforming to the structure of a concept map that will be used as a terminology mapping service.
   * @return
   */
  private def getCsvSchema(): CsvSchema = {
    CsvSchema.builder()
      .addColumn(ConceptMapFileColumns.SOURCE_SYSTEM)
      .addColumn(ConceptMapFileColumns.SOURCE_CODE)
      .addColumn(ConceptMapFileColumns.TARGET_SYSTEM)
      .addColumn(ConceptMapFileColumns.TARGET_CODE)
      .addColumn(ConceptMapFileColumns.TARGET_DISPLAY)
      .build()
      .withHeader()
      .withEscapeChar('\\')
  }

}
