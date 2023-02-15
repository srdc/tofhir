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
   *
   * @return
   */
  def getAllSchemaMetadata(projectId: String): Future[Seq[SchemaDefinition]]

  /**
   * Retrieve the schema identified by its id.
   *
   * @param projectId Project containing the schema definition
   * @param id        Identifier of the schema definition
   * @return
   */
  def getSchema(projectId: String, id: String): Future[Option[SchemaDefinition]]

  /**
   * Save the schema to the repository.
   *
   * @param projectId        Project containing the schema definition
   * @param schemaDefinition Content of the schema definition
   * @return
   */
  def saveSchema(projectId: String, schemaDefinition: SchemaDefinition): Future[SchemaDefinition]

  /**
   * Update the schema to the repository.
   *
   * @param projectId Project containing the schema definition
   * @param id               Identifier of the schema
   * @param schemaDefinition Content of the schema definition
   * @return
   */
  def updateSchema(projectId: String, id: String, schemaDefinition: SchemaDefinition): Future[Unit]

  /**
   * Delete the schema from the repository.
   *
   * @param projectId Project containing the schema definition
   * @param id Identifier of the schema definition
   * @return
   */
  def deleteSchema(projectId: String, id: String): Future[Unit]

}
