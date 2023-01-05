package io.tofhir.server.service

import com.typesafe.scalalogging.LazyLogging
import io.onfhir.api.Resource
import io.tofhir.server.model.{BadRequest, SchemaDefinition, SimpleStructureDefinition}
import io.tofhir.server.service.schema.{FolderSchemaRepository, ISchemaRepository}
import org.json4s.JArray
import org.json4s.JsonDSL._

import scala.concurrent.Future

class SchemaDefinitionService(schemaRepositoryFolderPath: String) extends LazyLogging {

  private val schemaRepository: ISchemaRepository = new FolderSchemaRepository(schemaRepositoryFolderPath)

  /**
   * Get all schema definition metadata (not populated with field definitions) from the schema repository
   *
   * @return A map of URL -> Seq[SimpleStructureDefinition]
   */
  def getAllMetadata(withReload: Boolean): Future[Seq[SchemaDefinition]] = {
    schemaRepository.getAllSchemaMetadata(withReload)
  }

  def getSchemaDefinition(url: String, withReload: Boolean): Future[Option[SchemaDefinition]] = {
    schemaRepository.getSchema(url, withReload)
  }

  /**
   * Create and save the schema definition to the schema repository.
   *
   * @param simpleStructureDefinition
   * @return
   */
  def createSchema(schemaDefinition: SchemaDefinition): Future[SchemaDefinition] = {
    schemaRepository.saveSchema(schemaDefinition)
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

}
