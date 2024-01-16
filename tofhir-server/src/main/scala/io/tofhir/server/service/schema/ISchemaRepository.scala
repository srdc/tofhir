package io.tofhir.server.service.schema

import io.tofhir.common.model.SchemaDefinition
import io.tofhir.engine.mapping.IFhirSchemaLoader
import org.json4s.JsonAST.JObject

import scala.concurrent.Future

/**
 * Interface to save and load SchemaDefinitions
 * so that the client applications can manage the schemas through CRUD operations
 */
trait ISchemaRepository extends IFhirSchemaLoader {

  /**
   * Retrieve the metadata of all SchemaDefinitions (only id, url, type and name fields are populated)
   *
   * @return
   */
  def getAllSchemas(projectId: String): Future[Seq[SchemaDefinition]]

  /**
   * Retrieve the schema identified by its id.
   *
   * @param projectId Project containing the schema definition
   * @param id        Identifier of the schema definition
   * @return
   */
  def getSchema(projectId: String, id: String): Future[Option[SchemaDefinition]]

  /**
   * Retrieve the schema identified by its url.
   * @param projectId Project containing the schema definition
   * @param url URL of the schema definition
   * @return
   */
  def getSchemaByUrl(projectId: String, url: String): Future[Option[SchemaDefinition]]

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

  /**
   * Deletes all schemas associated with a specific project.
   *
   * @param projectId The unique identifier of the project for which schemas should be deleted.
   */
  def deleteProjectSchemas(projectId: String): Unit

  /**
   *
   * @param projectId Project containing the schema definition
   * @param id Identifier of the schema definition
   * @return Structure definition of the schema
   */
  def getSchemaAsStructureDefinition(projectId: String, id: String): Future[Option[JObject]]

}
