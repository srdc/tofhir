package io.tofhir.engine.util.mappinggenerator

import io.tofhir.engine.util.mappinggenerator.GeneratorDBAdapter._

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import scala.collection.mutable

object GeneratorDBAdapter {
  private val DB_URL = "jdbc:postgresql://localhost:5432/ucl_omop"
  private val USERNAME = "postgres"
  private val PASSWORD = "password"

  private val COLUMN_CONCEPT_CODE: String = "concept_code"
  private val COLUMN_CONCEPT_OMOP_ID = "concept_id"
  private val COLUMN_CONCEPT_VOCABULARY_ID = "vocabulary_id"
  private val COLUMN_CONCEPT_NAME = "concept_name"

  private val COLUMN_CONCEPT_RELATIONSHIP_CONCEPT_ID_1 = "concept_id_1"
  private val COLUMN_CONCEPT_RELATIONSHIP_CONCEPT_ID_2 = "concept_id_2"
  private val COLUMN_CONCEPT_RELATIONSHIP_RELATIONSHIP_ID = "relationship_id"

  private val getConceptsForVocabulary: String = s"SELECT * FROM concept WHERE vocabulary_id = ?"
  private val getConceptRelationships: String = "SELECT * FROM concept_relationship cr" +
    " WHERE cr.concept_id_1 != cr.concept_id_2"
}

class GeneratorDBAdapter {
  val connection: Connection = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD)

  /**
   * Get concepts for a specific terminology, wrapped as [[OmopConcept]]s
   *
   * @param vocabularyId OMOP vocabulary id for the terminology system, for which the concepts to be obtained
   * @param conceptDomain Domain of the concepts of interests as defined in the OMOP vocabulary e.g. Conditin, Procedure, etc.
   * @return
   */
  def getConcepts(vocabularyId: String, conceptDomain: String): Set[OmopConcept] = {
    var statement: PreparedStatement = null
    try {
      statement = connection.prepareStatement(s"$getConceptsForVocabulary AND domain_id = ? ")
      statement.setString(1, vocabularyId)
      statement.setString(2, conceptDomain)
      getOmopConcepts(statement.executeQuery())
    }
    finally {
      statement.close()
    }
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

  /**
   * Retrieves all the relationships, specified by the input parameter, from the OMOP vocabulary
   * @param relationships Names as defined in the OMOP vocabulary of the relationships to be retrieved
   * @return
   */
  def getOmopConceptRelationships(relationships: Set[String]): Set[OmopConceptRelationship] = {
    val statement: PreparedStatement = prepareRelationshipStatement(relationships)
    val resultSet: ResultSet = statement.executeQuery()
    try {
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

    } finally {
      resultSet.close()
      statement.close()
    }
  }

  private def prepareRelationshipStatement(relationships: Set[String]): PreparedStatement = {
    val relationshipCriteria: String = relationships.map(_ => s"cr.relationship_id = ?").mkString(" AND ")
    val statement: PreparedStatement = connection.prepareStatement(s"$getConceptRelationships AND $relationshipCriteria")
    relationships
      .zipWithIndex
      .foreach(r => statement.setString(r._2 + 1, r._1))
    statement
  }

  def clear(): Unit = {
    connection.close()
  }
}
