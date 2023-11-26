package io.tofhir.engine.util

import com.fasterxml.jackson.dataformat.csv.{CsvMapper, CsvSchema}
import io.tofhir.engine.mapping.LocalTerminologyService.ConceptMapFileColumns
import io.tofhir.engine.util.TerminologySystemMappingGenerator._

import java.io.File
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import scala.beans.BeanProperty
import scala.collection.mutable
import scala.jdk.CollectionConverters.IterableHasAsJava

object TerminologySystemMappingGenerator {
  private val DB_URL = "jdbc:postgresql://localhost:5432/ucl_omop"
  private val USERNAME = "postgres"
  private val PASSWORD = "password"

  private val OMOP_URL: String = "https://www.ohdsi.org/omop"

  private val COLUMN_CONCEPT_CODE: String = "concept_code"
  private val COLUMN_CONCEPT_OMOP_ID = "concept_id"
  private val COLUMN_CONCEPT_VOCABULARY_ID = "vocabulary_id"
  private val COLUMN_CONCEPT_NAME = "concept_name"

  private val COLUMN_CONCEPT_RELATIONSHIP_CONCEPT_ID_1 = "concept_id_1"
  private val COLUMN_CONCEPT_RELATIONSHIP_CONCEPT_ID_2 = "concept_id_2"
  private val COLUMN_CONCEPT_RELATIONSHIP_RELATIONSHIP_ID = "relationship_id"

  private val getConceptsForVocabulary: String = s"SELECT * FROM concept WHERE vocabulary_id = ?"
  private val getConceptRelationships: String = "SELECT * FROM concept_relationship cr" +
    " WHERE cr.concept_id_1 != cr.concept_id_2 AND cr.relationship_id = 'Mapped from'"

  /**
   * Example usage
   */
  def main(args: Array[String]): Unit = {
    new TerminologySystemMappingGenerator().generateMappings("SNOMED", "ICD10CM", "http://hl7.org/fhir/sid/icd-10", "icd10cm.csv")
  }

}

/**
 * A class to generate file-based terminology mappings by parsing the relationships defined in the OMOP vocabulary.
 * The generator creates terminology mappings from a source terminology to a target one e.g. from SNOMED to ICD10.
 */
class TerminologySystemMappingGenerator {
  // All concepts defined in the OMOP vocabulary associated with the source and target terminologies.
  // It is a map from the OMOP concept id to the concept object including the details about the concept.
  var concepts: mutable.Map[Int, OmopConcept] = mutable.Map.empty
  // All relationships defined in the OMOP vocabulary. The relationships are represented as map of (OMOP concept id to set of OMOP concepts) entries where the
  // key represents the identifier of the source concept and set of concepts represent the concepts from the target terminology system equivalent to the source concept.
  var relationships: mutable.Map[Int, mutable.Set[OmopConcept]] = mutable.Map.empty

  /**
   * Generates a file including terminology translations from a source code system to a target code system.
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
   * @param sourceSystem    OMOP vocabulary ID for the source terminology
   * @param targetSystem    OMOP vocabulary ID for the target terminology
   * @param targetSystemUrl URL for the target vocabulary. It will be set as the URL of the target codes
   * @param outputFile      Name of the output file
   */
  def generateMappings(sourceSystem: String, targetSystem: String, targetSystemUrl: String, outputFile: String): Unit = {
    val connection: Connection = getConnection()

    // Retrieve all the concepts associated with the source and target code systems (terminologies) and all relationships are obtained from the OMOP vocabulary
    populateConceptsAndRelationships(sourceSystem, targetSystem, connection)

    // Each source system concept is a starting point to initiate a mapping identification process
    val sourceSystemConcepts: Set[OmopConcept] = getConcepts(sourceSystem, connection)
    // Visited concepts will keep the concepts that are already considered so that they won't be considered again. This case might happen
    // when a concept is considered already while identifiying mappings for an upstream concept. For example:
    // Assuming we have such a mapping structure: SNOMED code 1 -> SNOMED code 2 -> ICD, indicating that SNOMED code 1 maps to SNOMED code 2; and SNOMED code 2
    // maps to an ICD code. While getting the mappings for SNOMED code 1, we will have processed the SNOMED code 2 as well. Therefore, it won't need to be processed once again.
    val visitedConcepts: mutable.Set[OmopConcept] = mutable.Set.empty[OmopConcept]
    // Keeps the terminology system concepts to be outputed to the CSV file
    val terminologySystemConcepts: mutable.Set[TerminologySystemMapping] = mutable.Set.empty[TerminologySystemMapping]

    // Find mappings for each source concept unless it's been processed already
    sourceSystemConcepts.toSeq.foreach(concept => {
      if (!visitedConcepts.contains(concept)) {
        val mappings: Set[TerminologySystemMapping] = findMappings(concept, targetSystem, targetSystemUrl, visitedConcepts)
        terminologySystemConcepts.addAll(mappings)
        if (mappings.nonEmpty) {
          println(s"Retrieved ${mappings.size} mappings for concept: $concept")
        }
      }
    })

    connection.close()

    generateTerminologySystemFile(terminologySystemConcepts.toSet, outputFile)
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
   * Get concepts for a specific terminology, wrapped as [[OmopConcept]]s
   *
   * @param vocabularyId OMOP vocabulary id for the terminology system, for which the concepts to be obtained
   * @param connection
   * @return
   */
  private def getConcepts(vocabularyId: String, connection: Connection): Set[OmopConcept] = {
    val statement: PreparedStatement = connection.prepareStatement(getConceptsForVocabulary)
    statement.setString(1, vocabularyId)
    getOmopConcepts(statement.executeQuery())
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
    val aggregatedMappings: mutable.Set[OmopConcept] = mutable.Set.empty[OmopConcept]
    val visitQueue: mutable.Queue[OmopConcept] = mutable.Queue.empty[OmopConcept]

    visitQueue.enqueue(sourceConcept)
    while (visitQueue.nonEmpty) {
      val currentCode: OmopConcept = visitQueue.dequeue()
      if (!visitedConcepts.contains(currentCode)) {
        visitedConcepts.add(currentCode)
        relationships.get(currentCode.concept_id) match {
          case None =>
          case Some(mappings) =>
            mappings.foreach(concept => {
              if (concept.vocabulary_id.equals(targetSystem)) {
                aggregatedMappings.add(concept)
              } else {
                visitQueue.enqueue(concept)
              }
            })
        }
      }
    }
    aggregatedMappings.map(mapping => TerminologySystemMapping(OMOP_URL, sourceConcept.concept_id.toString, targetSystemUrl, mapping.concept_code, mapping.concept_name)).toSet
  }

  /**
   * Obtains concepts for the source and target terminologies and relationships as defined in the OMOP vocabulary.
   *
   * @param sourceSystem
   * @param targetSystem
   * @param connection
   */
  private def populateConceptsAndRelationships(sourceSystem: String, targetSystem: String, connection: Connection): Unit = {
    var statement: PreparedStatement = connection.prepareStatement(getConceptsForVocabulary)
    statement.setString(1, sourceSystem)
    getOmopConcepts(statement.executeQuery()).foreach(concept => concepts.put(concept.concept_id, concept))
    statement.clearParameters()
    statement.setString(1, targetSystem)
    getOmopConcepts(statement.executeQuery()).foreach(concept => concepts.put(concept.concept_id, concept))
    statement.close()

    statement = connection.prepareStatement(getConceptRelationships)
    getOmopConceptRelationships(statement.executeQuery())
      .foreach(relationship => {
        val conceptMappings: mutable.Set[OmopConcept] = relationships.getOrElseUpdate(relationship.concept_id_1, mutable.Set.empty[OmopConcept])
        if (concepts.contains(relationship.concept_id_2)) {
          conceptMappings.add(concepts(relationship.concept_id_2))
        }
      })
  }

  private def getOmopConcepts(resultSet: ResultSet): Set[OmopConcept] = {
    val concepts: mutable.Set[OmopConcept] = mutable.Set.empty
    while (resultSet.next()) {
      concepts.add(
        OmopConcept(
          resultSet.getInt(COLUMN_CONCEPT_OMOP_ID),
          resultSet.getString(COLUMN_CONCEPT_VOCABULARY_ID),
          resultSet.getString(COLUMN_CONCEPT_CODE),
          resultSet.getString(COLUMN_CONCEPT_NAME)
        )
      )
    }
    concepts.toSet
  }

  private def getOmopConceptRelationships(resultSet: ResultSet): Set[OmopConceptRelationship] = {
    val relationships: mutable.Set[OmopConceptRelationship] = mutable.Set.empty
    while (resultSet.next()) {
      relationships.add(
        OmopConceptRelationship(
          resultSet.getInt(COLUMN_CONCEPT_RELATIONSHIP_CONCEPT_ID_1),
          resultSet.getInt(COLUMN_CONCEPT_RELATIONSHIP_CONCEPT_ID_2),
          resultSet.getString(COLUMN_CONCEPT_RELATIONSHIP_RELATIONSHIP_ID)
        )
      )
    }
    relationships.toSet
  }

  private def getConnection(): Connection = {
    DriverManager.getConnection(DB_URL, USERNAME, PASSWORD)
  }

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

case class OmopConcept(concept_id: Int, vocabulary_id: String, concept_code: String, concept_name: String)

case class OmopConceptRelationship(concept_id_1: Int, concept_id_2: Int, relationship_id: String)

case class TerminologySystemMapping(@BeanProperty source_system: String,
                                    @BeanProperty source_code: String,
                                    @BeanProperty target_system: String,
                                    @BeanProperty target_code: String,
                                    @BeanProperty target_display: String)
