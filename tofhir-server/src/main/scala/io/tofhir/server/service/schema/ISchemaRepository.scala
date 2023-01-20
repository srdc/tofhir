package io.tofhir.server.service.schema

import io.tofhir.server.model.SchemaDefinition

import scala.concurrent.Future

/**
 * Interface to save and load SchemaDefinitions
 * so that the client applications can manage the schemas through CRUD operations
 */
trait ISchemaRepository {

  /**
   * Retrieve the metadata of all SchemaDefinitions (only url, type and name fields are populated)
   * @param withReload If true, reload all definitions from the repository implementation (from disk)
   * @return
   */
  def getAllSchemaMetadata(withReload: Boolean): Future[Seq[SchemaDefinition]]

  /**
   * Retrieve the schema identified by its URL.
   * @param url
   * @param withReload If true, reload all definitions from the repository implementation (from disk)
   * @return
   */
  def getSchemaByUrl(url: String, withReload: Boolean): Future[Option[SchemaDefinition]]

  /**
   * Retrieve the schema identified by its type.
   * @param name
   * @param withReload If true, reload all definitions from the repository implementation (from disk)
   * @return
   */
  def getSchemaByName(name: String, withReload: Boolean): Future[Option[SchemaDefinition]]

  /**
   * Save the schema to the repository.
   * @param schemaDefinition
   * @return
   */
  def saveSchema(schemaDefinition: SchemaDefinition): Future[SchemaDefinition]

  /**
   * Update the schema to the repository.
   * @param name
   * @return
   */
  def putSchema(name: String, schemaDefinition: SchemaDefinition): Future[Unit]

  /**
   * Delete the schema from the repository.
   * @param name
   * @return
   */
  def deleteSchema(name: String): Future[Unit]

}
