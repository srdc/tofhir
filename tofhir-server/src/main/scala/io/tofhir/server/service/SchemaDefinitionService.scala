package io.tofhir.server.service

import com.typesafe.scalalogging.LazyLogging
import io.onfhir.api.{FHIR_DATA_TYPES, FHIR_ROOT_URL_FOR_DEFINITIONS, Resource}
import io.tofhir.common.model.{DataTypeWithProfiles, SchemaDefinition, SimpleStructureDefinition}
import io.tofhir.engine.data.read.SourceHandler
import io.tofhir.server.config.SparkConfig
import io.tofhir.server.model.{BadRequest, InferTask}
import io.tofhir.server.service.schema.ISchemaRepository
import org.apache.spark.sql.types.DataTypes
import org.json4s.JArray
import org.json4s.JsonDSL._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class SchemaDefinitionService(schemaRepository: ISchemaRepository) extends LazyLogging {

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
   * @return
   */
  def deleteSchema(projectId: String, id: String): Future[Unit] = {
    schemaRepository.deleteSchema(projectId, id)
  }

  private def convertToFhirResource(schemaUrl: String, name: String, `type`: String, rootPath: String, fieldDefinitions: Seq[SimpleStructureDefinition]): Resource = {
    val structureDefinitionResource: Resource =
      ("resourceType" -> "StructureDefinition") ~
        ("url" -> schemaUrl) ~
        ("name" -> name) ~
        ("status" -> "draft") ~
        ("fhirVersion" -> "4.0.1") ~
        ("kind" -> "logical") ~
        ("abstract" -> false) ~
        ("type" -> `type`) ~
        ("baseDefinition" -> "http://hl7.org/fhir/StructureDefinition/Element") ~
        ("derivation" -> "specialization") ~
        ("differential" -> ("element" -> generateElementArray(rootPath, fieldDefinitions)))

    structureDefinitionResource
  }

  private def generateElementArray(rootPath: String, fieldDefinitions: Seq[SimpleStructureDefinition]): JArray = {
    // Check whether all field definitions have at least one data type
    val integrityCheck = fieldDefinitions.forall(fd => fd.dataTypes.isDefined && fd.dataTypes.get.nonEmpty)
    if (!integrityCheck) {
      throw BadRequest("Missing data type.", s"A field definition must have at least one data type. Element rootPath: ${rootPath}")
    }

    val rootElement =
      ("id" -> rootPath) ~
        ("path" -> rootPath) ~
        ("min" -> 0) ~
        ("max" -> "*") ~
        ("type" -> JArray(List("code" -> "Element")))

    val elements = fieldDefinitions.map { fd =>
      ("id" -> fd.id) ~
        ("path" -> fd.path) ~
        ("short" -> fd.short) ~
        ("definition" -> fd.definition) ~
        ("min" -> fd.minCardinality) ~
        ("max" -> fd.maxCardinality) ~
        ("type" -> fd.dataTypes.get.map { dt =>
          ("code" -> dt.dataType) ~
            ("profile" -> dt.profiles)
        })
    }.toList

    JArray(rootElement +: elements)
  }

  /**
   * Infer the schema
   * @param inferTask infer schema task object
   * @return
   */
  def inferSchema(inferTask: InferTask): Future[Option[SchemaDefinition]] = {

    // Execute SQL and get the dataFrame
    val dataFrame = SourceHandler.readSource("unnamed", SparkConfig.sparkSession,
      inferTask.sqlSource, inferTask.sourceSettings.head._2, None, None)

    // Create unnamed Schema definition by infer the schema from DataFrame
    val unnamedSchema = {

      // Map SQL DataTypes to Fhir DataTypes
      var fieldDefinitions = dataFrame.schema.fields.map(structField => {
        SimpleStructureDefinition(
          id = structField.name,
          path = "unnamed." + structField.name,
          dataTypes = structField.dataType match {
            case DataTypes.ShortType => Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.INTEGER, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.INTEGER}")))))
            case DataTypes.LongType => Some(Seq(DataTypeWithProfiles(dataType = "integer64", profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/integer64")))))
            case DataTypes.StringType => Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.STRING, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.STRING}")))))
            case DataTypes.IntegerType => Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.INTEGER, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.INTEGER}")))))
            case DataTypes.ByteType => Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.INTEGER, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.INTEGER}")))))
            case DataTypes.BinaryType => Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.BASE64BINARY, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.BASE64BINARY}")))))
            case DataTypes.BooleanType => Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.BOOLEAN, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.BOOLEAN}")))))
            case DataTypes.CalendarIntervalType => Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.DATETIME, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.DATETIME}")))))
            case DataTypes.DateType => Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.DATE, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.DATE}")))))
            case DataTypes.DoubleType => Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.DECIMAL, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.DECIMAL}")))))
            case DataTypes.FloatType => Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.DECIMAL, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.DECIMAL}")))))
            case DataTypes.NullType => Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.STRING, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.STRING}")))))
            case DataTypes.TimestampType => Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.DATETIME, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.DATETIME}")))))
          },
          isPrimitive = true,
          isChoiceRoot = false,
          isArray = false,
          minCardinality = 0,
          maxCardinality = Some(1),
          boundToValueSet = None,
          isValueSetBindingRequired = None,
          referencableProfiles = None,
          constraintDefinitions = None,
          sliceDefinition = None,
          sliceName = None,
          fixedValue = None,
          patternValue = None,
          referringTo = None,
          short = Some(structField.name),
          definition = Some(structField.name),
          comment = None,
          elements = None
        )
      })

      // Remove INPUT_VALIDITY_ERROR fieldDefinition that is added by SourceHandler
      fieldDefinitions = fieldDefinitions.filter(fieldDefiniton => fieldDefiniton.id != SourceHandler.INPUT_VALIDITY_ERROR)

      SchemaDefinition(url = "unnamed", `type` = "unnamed", name = "unnamed", rootDefinition = Option.empty, fieldDefinitions = Some(fieldDefinitions))
    }

    Future.apply(Some(unnamedSchema))
  }
}
