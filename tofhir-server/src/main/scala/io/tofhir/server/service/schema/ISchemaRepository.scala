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
  def getSchema(url: String, withReload: Boolean): Future[Option[SchemaDefinition]]

  /**
   * Retrieve the schema identified by its type.
   * @param `type`
   * @param withReload If true, reload all definitions from the repository implementation (from disk)
   * @return
   */
  def getSchemaByType(`type`: String, withReload: Boolean): Future[Option[SchemaDefinition]]

  /**
   * Save the schema to the repository.
   * @param schemaDefinition
   * @return
   */
  def saveSchema(schemaDefinition: SchemaDefinition): Future[SchemaDefinition]

}
