package io.tofhir.server.repository.schema

import io.onfhir.api.Resource
import io.onfhir.definitions.common.model.SchemaDefinition
import io.tofhir.engine.mapping.schema.IFhirSchemaLoader
import io.tofhir.engine.repository.ICachedRepository
import io.tofhir.server.repository.project.IProjectList

import scala.concurrent.Future

/**
 * Interface to save and load SchemaDefinitions
 * so that the client applications can manage the schemas through CRUD operations
 */
trait ISchemaRepository extends IFhirSchemaLoader with ICachedRepository with IProjectList[SchemaDefinition] {

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
   * @param schemaId  Identifier of the schema definition
   * @return
   */
  def getSchema(projectId: String, schemaId: String): Future[Option[SchemaDefinition]]

  /**
   * Retrieve the schema identified by its url.
   *
   * @param url URL of the schema definition
   * @return
   */
  def getSchemaByUrl(url: String): Future[Option[SchemaDefinition]]

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
   * @param projectId        Project containing the schema definition
   * @param schemaId         Identifier of the schema
   * @param schemaDefinition Content of the schema definition
   * @return
   */
  def updateSchema(projectId: String, schemaId: String, schemaDefinition: SchemaDefinition): Future[SchemaDefinition]

  /**
   * Delete the schema from the repository.
   *
   * @param projectId Project containing the schema definition
   * @param schemaId  Identifier of the schema definition
   * @return
   */
  def deleteSchema(projectId: String, schemaId: String): Future[Unit]

  /**
   * Deletes all schemas associated with a specific project.
   *
   * @param projectId The unique identifier of the project for which schemas should be deleted.
   */
  def deleteAllSchemas(projectId: String): Future[Unit]

  /**
   * Retrieve the Structure Definition of the schema identified by its id.
   *
   * @param projectId Project containing the schema definition
   * @param schemaId  Identifier of the schema definition
   * @return Structure definition of the schema converted into StructureDefinition Resource
   */
  def getSchemaAsStructureDefinition(projectId: String, schemaId: String): Future[Option[Resource]]


  /**
   * Saves the schemas by using their Structure Definition resources.
   *
   * @param projectId                    The ID of the project that contains the schema definition.
   * @param structureDefinitionResources A sequence of resources representing the structure definitions to be saved.
   * @return A Future containing a sequence of SchemaDefinition objects for the created schemas.
   */
  def saveSchemaByStructureDefinition(projectId: String, structureDefinitionResources: Seq[Resource]): Future[Seq[SchemaDefinition]]

}
