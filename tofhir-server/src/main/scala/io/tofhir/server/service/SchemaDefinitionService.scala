package io.tofhir.server.service

import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.typesafe.scalalogging.LazyLogging
import io.onfhir.api.Resource
import io.onfhir.client.util.FhirClientUtil
import io.onfhir.definitions.common.model.SchemaDefinition
import io.tofhir.engine.Execution.actorSystem
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.config.ToFhirConfig
import io.tofhir.engine.data.read.SourceHandler
import io.tofhir.engine.mapping.schema.SchemaConverter
import io.tofhir.engine.model.{SqlSource, SqlSourceSettings}
import io.tofhir.engine.model.exception.FhirMappingException
import io.tofhir.engine.util.redcap.RedCapUtil
import io.tofhir.engine.util.{CsvUtil, FhirVersionUtil}
import io.tofhir.server.common.model.{BadRequest, ResourceNotFound}
import io.tofhir.server.model.{ImportSchemaSettings, InferTask}
import io.tofhir.server.repository.mapping.IMappingRepository
import io.tofhir.server.repository.schema.ISchemaRepository
import org.apache.hadoop.shaded.org.apache.http.HttpStatus
import org.apache.spark.sql.types.StructType

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
   * @return
   */
  def getSchema(projectId: String, id: String): Future[Option[SchemaDefinition]] = {
    schemaRepository.getSchema(projectId, id)
  }

  /**
   * Get a schema definition by its URL from the schema repository
   *
   * @param url URL of the schema definition
   * @return
   */
  def getSchemaByUrl(url: String): Future[Option[SchemaDefinition]] = {
    schemaRepository.getSchemaByUrl(url)
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
   * Update the schema definition in the schema repository.
   *
   * @param projectId
   * @param schemaId
   * @param schemaDefinition
   * @return
   */
  def putSchema(projectId: String, schemaId: String, schemaDefinition: SchemaDefinition): Future[SchemaDefinition] = { // TODO: HTTP PUT methods should return the updated object w.r.t REST principles
    // Ensure that the provided schemaId matches the SchemaDefinition's schemaId
    if (!schemaId.equals(schemaDefinition.id)) {
      throw BadRequest("Schema definition is not valid.", s"Identifier of the schema definition: ${schemaDefinition.id} does not match with the provided schemaId: $schemaId in the path!")
    }
    schemaRepository.updateSchema(projectId, schemaId, schemaDefinition)
  }

  /**
   * Delete the schema definition from the schema repository.
   *
   * @param projectId
   * @param schemaId
   * @throws ResourceNotFound when the schema does not exist
   * @throws BadRequest       when the schema is in use by some mappings
   * @return
   */
  def deleteSchema(projectId: String, schemaId: String): Future[Unit] = {
    schemaRepository.getSchema(projectId, schemaId).flatMap(schema => {
      if (schema.isEmpty)
        throw ResourceNotFound("Schema does not exists.", s"A schema definition with id $schemaId does not exists in the schema repository.")
      mappingRepository.getMappingsReferencingSchema(projectId, schema.get.url).flatMap(mappingIds => {
        if (mappingIds.isEmpty)
          schemaRepository.deleteSchema(projectId, schemaId)
        else
          throw BadRequest("Schema is referenced by some mappings.", s"Schema definition with id $schemaId is referenced by the following mappings:${mappingIds.mkString(",")}")
      })
    })
  }

  /**
   * It takes inferTask object, connects to database, and executes the query. Infer a SchemaDefinition object from the result of the query.
   *
   * @param inferTask The object that contains database connection information and sql query
   * @return SchemaDefinition object containing the field type information
   */
  def inferSchema(inferTask: InferTask): Future[Option[SchemaDefinition]] = {

    // Initialize a SchemaConverter for our FHIR version
    val schemaConverter = new SchemaConverter(majorFhirVersion = FhirVersionUtil.getMajorFhirVersion(ToFhirConfig.engineConfig.schemaRepositoryFhirVersion))

    // Extract the first mapping job source setting
    val sourceSettings = inferTask.mappingJobSourceSettings
      .head // We are sure that we only have a single sourceSetting since this is an infer task
      ._2 // Get the MappingJobSourceSettings object

    // Inner function to call from multiple lines below so that the schema is inferred after read by spark.
    def inferSchemaFromSparkRead(): StructType = {
      // Execute SQL and get the dataFrame
      val dataFrame = try {
        SourceHandler
          .readSource(inferTask.name, ToFhirConfig.sparkSession, inferTask.sourceBinding, sourceSettings, schema = None)
          .limit(1) // It is enough to take the first row to infer the schema.
      } catch {
        case e: FhirMappingException =>
          // Remove the new lines and capitalize the error detail to show it in front-end properly.
          throw BadRequest(e.getMessage, e.getCause.toString.capitalize.replace("\n", " "))
      }
      dataFrame.schema // Get the schema inferred by Spark
    }

    val effectiveSchema = sourceSettings match {
      case sqlSourceSettings: SqlSourceSettings =>
        inferTask.sourceBinding match {
          case sqlSource: SqlSource if sqlSource.preprocessSql.isEmpty => // If there is a preprocessSq
            try {
              schemaConverter
                .getTableSchema(
                  sqlSourceSettings.databaseUrl,
                  sqlSourceSettings.username,
                  sqlSourceSettings.password,
                  sqlSource.tableName.getOrElse(sqlSource.query.get),
                  sqlSource.query.isDefined)
            } catch {
              case e: Throwable =>
                throw BadRequest(e.getMessage, e.getCause.toString, Some(e))
            }
          case sqlSource: SqlSource if sqlSource.preprocessSql.isDefined =>
            inferSchemaFromSparkRead()
          case _ => throw new IllegalStateException("Source binding must be SqlSource if the sourceSettings is SqlSourceSettings.")
        }
      case _ =>
        inferSchemaFromSparkRead()
    }

    // Default name for undefined information
    val defaultName: String = "Unnamed"
    // Create unnamed Schema definition by infer the schema from DataFrame
    val unnamedSchema = {
      // Map SQL DataTypes to Fhir DataTypes
      var fieldDefinitions = effectiveSchema.fields.map(structField => schemaConverter.fieldsToSchema(structField, defaultName))
      // Remove INPUT_VALIDITY_ERROR fieldDefinition that is added by SourceHandler
      fieldDefinitions = fieldDefinitions.filter(fieldDefinition => fieldDefinition.id != SourceHandler.INPUT_VALIDITY_ERROR)
      SchemaDefinition(url = defaultName, version = SchemaDefinition.VERSION_LATEST, `type` = defaultName, name = defaultName, description = Option.empty, rootDefinition = Option.empty, fieldDefinitions = Some(fieldDefinitions))
    }
    Future.apply(Some(unnamedSchema))
  }

  /**
   * Imports a schema from the specified FHIR server using the provided settings.
   *
   * @param projectId The ID of the project.
   * @param settings  The settings for importing the schema.
   * @return A Future containing the imported SchemaDefinition.
   * @throws BadRequest if the resource identifier is invalid or does not exist.
   */
  @throws[BadRequest]
  def importSchema(projectId: String, settings: ImportSchemaSettings): Future[SchemaDefinition] = {
    // get the OnFhir Client using the given settings
    val onFhirClient = FhirClientUtil.createOnFhirClient(settings.baseUrl, settings.securitySettings)
    // fetch the structure definition
    onFhirClient.read("StructureDefinition", settings.resourceId).execute() flatMap { response =>
      // the requested resource does not exist
      if (response.httpStatus.intValue() == HttpStatus.SC_NOT_FOUND)
        throw BadRequest("Invalid resource identifier!", s"Structure Definition with id '${settings.resourceId}' does not exist.")
      // save the schema
      schemaRepository.saveSchemaByStructureDefinition(projectId, Seq(response.responseBody.get))
        .map(definitions => definitions.head)
    }
  }

  /**
   * Creates schema definitions for the given FHIR resources.
   *
   * @param projectId The ID of the project in which the schema definitions will be created.
   * @param resources A sequence of FHIR resources representing the schema definitions to be created.
   * @return A Future containing a sequence of `SchemaDefinition` objects for the created schemas.
   */
  def createSchemas(projectId: String, resources: Seq[Resource]): Future[Seq[SchemaDefinition]] = {
    schemaRepository.saveSchemaByStructureDefinition(projectId, resources)
  }

  /**
   * Imports a REDCap Data Dictionary file to create new schemas for the forms defined in the given file.
   *
   * @param projectId     project id for which the schemas will be created
   * @param byteSource    the REDCap Data Dictionary File
   * @param rootUrl       the root URL of the schemas to be created
   * @param recordIdField The name of the field that represents the record ID.
   * @return
   */
  def importREDCapDataDictionary(projectId: String, byteSource: Source[ByteString, Any], rootUrl: String, recordIdField: String): Future[Seq[SchemaDefinition]] = {
    // read the file
    val content: Future[Seq[Map[String, String]]] = CsvUtil.readFromCSVSource(byteSource)
    content.flatMap(rows => {
      // extract schema definitions
      val definitions: Seq[SchemaDefinition] = RedCapUtil.extractSchemasAsSchemaDefinitions(rows, rootUrl, recordIdField)
      // save each schema
      Future.sequence(definitions.map(definition => schemaRepository.saveSchema(projectId, definition)))
    })
  }

  /**
   * Get structure definition resource of the schema
   *
   * @param projectId project containing the schema definition
   * @param id        id of the requested schema
   * @return Structure definition of the schema converted into StructureDefinition Resource
   */
  def getSchemaAsStructureDefinition(projectId: String, id: String): Future[Option[Resource]] = {
    schemaRepository.getSchemaAsStructureDefinition(projectId, id)
  }

  /**
   * Save the schema by using its StructureDefinition
   *
   * @param projectId                   Identifier of the project in which the schema will be created
   * @param structureDefinitionResource schema definition in the form of Structure Definition resource
   * @return the SchemaDefinition of the created schema
   */
  def createSchemaFromStructureDefinition(projectId: String, structureDefinitionResource: Resource): Future[SchemaDefinition] = {
    schemaRepository.saveSchemaByStructureDefinition(projectId, Seq(structureDefinitionResource))
      .map(definitions => definitions.head)
  }
}
