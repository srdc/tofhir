package io.tofhir.server.service

import com.typesafe.scalalogging.LazyLogging
import io.tofhir.common.model.SchemaDefinition
import io.tofhir.engine.data.read.SourceHandler
import io.tofhir.server.config.SparkConfig
import io.tofhir.server.model.{BadRequest, InferTask, ResourceNotFound}
import io.tofhir.server.service.schema.ISchemaRepository
import io.tofhir.engine.mapping.SchemaConverter
import io.tofhir.server.service.mapping.IMappingRepository

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class SchemaDefinitionService(schemaRepository: ISchemaRepository, mappingRepository: IMappingRepository) extends LazyLogging {

  /**
   * Get all schema definition metadata (not populated with field definitions) from the schema repository
   *
   * @return A map of URL -> Seq[SimpleStructureDefinition]
   */
  def getAllSchemas(projectId: String): Future[Seq[SchemaDefinition]] = {
    schemaRepository.getAllSchemas(projectId)
  }

  /**
   * Get a schema definition from the schema repository
   *
   * @param projectId
   * @param id
   *
   * @return
   */
  def getSchema(projectId: String, id: String): Future[Option[SchemaDefinition]] = {
    schemaRepository.getSchema(projectId, id)
  }

  /**
   * Get a schema definition by its URL from the schema repository
   * @param projectId Project containing the schema definition
   * @param url URL of the schema definition
   * @return
   */
  def getSchemaByUrl(projectId: String, url: String): Future[Option[SchemaDefinition]] = {
    schemaRepository.getSchemaByUrl(projectId, url)
  }

  /**
   * Create and save the schema definition to the schema repository.
   *
   * @param projectId
   * @param simpleStructureDefinition
   * @return
   */
  def createSchema(projectId: String, schemaDefinition: SchemaDefinition): Future[SchemaDefinition] = {
    schemaRepository.saveSchema(projectId, schemaDefinition)
  }

  /**
   * Update the schema definition to the schema repository.
   *
   * @param projectId
   * @param id
   * @param schemaDefinition
   * @return
   */
  def putSchema(projectId: String, id: String, schemaDefinition: SchemaDefinition): Future[Unit] = {
    schemaRepository.updateSchema(projectId, id, schemaDefinition)
  }

  /**
   * Delete the schema definition from the schema repository.
   *
   * @param projectId
   * @param id
   * @throws ResourceNotFound when the schema does not exist
   * @throws BadRequest when the schema is in use by some mappings
   * @return
   */
  def deleteSchema(projectId: String, id: String): Future[Unit] = {
    schemaRepository.getSchema(projectId, id).flatMap(schema => {
      if (schema.isEmpty)
        throw ResourceNotFound("Schema does not exists.", s"A schema definition with id $id does not exists in the schema repository.")
      mappingRepository.getMappingsReferencingSchema(projectId,schema.get.url).flatMap(mappingIds => {
        if (mappingIds.isEmpty)
          schemaRepository.deleteSchema(projectId, id)
        else
          throw BadRequest("Schema is referenced by some mappings.",s"Schema definition with id $id is referenced by the following mappings:${mappingIds.mkString(",")}")
      })
    })
  }

  /**
   * It takes inferTask object, connects to database, and executes the query. Infer a SchemaDefinition object from the result of the query.
   * @param inferTask The object that contains database connection information and sql query
   * @return SchemaDefinition object containing the field type information
   */
  def inferSchema(inferTask: InferTask): Future[Option[SchemaDefinition]] = {
    // Execute SQL and get the dataFrame
    val dataFrame = SourceHandler.readSource(inferTask.name, SparkConfig.sparkSession,
      inferTask.sourceContext, inferTask.sourceSettings.head._2, None, None, Some(1))
    // Default name for undefined information
    val defaultName: String = "unnamed"
    // Create unnamed Schema definition by infer the schema from DataFrame
    val unnamedSchema = {
      // Schema converter object for mapping spark data types to fhir data types
      val schemaConverter = new SchemaConverter(majorFhirVersion = "R4")
      // Map SQL DataTypes to Fhir DataTypes
      var fieldDefinitions = dataFrame.schema.fields.map(structField => schemaConverter.fieldsToSchema(structField, defaultName))
      // Remove INPUT_VALIDITY_ERROR fieldDefinition that is added by SourceHandler
      fieldDefinitions = fieldDefinitions.filter(fieldDefinition => fieldDefinition.id != SourceHandler.INPUT_VALIDITY_ERROR)
      SchemaDefinition(url = defaultName, `type` = defaultName, name = defaultName, rootDefinition = Option.empty, fieldDefinitions = Some(fieldDefinitions))
    }
    Future.apply(Some(unnamedSchema))
  }
}
