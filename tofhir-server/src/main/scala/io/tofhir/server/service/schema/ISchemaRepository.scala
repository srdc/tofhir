package io.tofhir.server.service.schema

import io.tofhir.server.model.SchemaDefinition

import scala.concurrent.Future

/**
 * Interface to save and load SchemaDefinitions
 * so that the client applications can manage the schemas through CRUD operations
 */
trait ISchemaRepository {

  /**
   * Retrieve the metadata of all SchemaDefinitions (only id, url, type and name fields are populated)
   * @return
   */
  def getAllSchemaMetadata(projectId: String): Future[Seq[SchemaDefinition]]

  /**
   * Retrieve the schema identified by its id.
   * @param id
   * @return
   */
  def getSchema(id: String): Future[Option[SchemaDefinition]]

  /**
   * Save the schema to the repository.
   * @param schemaDefinition
   * @return
   */
  def saveSchema(schemaDefinition: SchemaDefinition): Future[SchemaDefinition]

  /**
   * Update the schema to the repository.
   * @param id
   * @return
   */
  def putSchema(id: String, schemaDefinition: SchemaDefinition): Future[Unit]

  /**
   * Delete the schema from the repository.
   * @param id
   * @return
   */
  def deleteSchema(id: String): Future[Unit]

}
