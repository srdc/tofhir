package io.tofhir.server.service.schema

import io.onfhir.api
import io.onfhir.api.util.IOUtil
import io.onfhir.api.validation.ProfileRestrictions
import io.onfhir.config.{BaseFhirConfig, FSConfigReader, IFhirConfigReader}
import io.onfhir.util.JsonFormatter._
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.model.FhirMappingException
import io.tofhir.engine.util.FileUtils
import io.tofhir.engine.util.FileUtils.FileExtensions
import io.tofhir.server.model.{AlreadyExists, BadRequest, ResourceNotFound, SchemaDefinition}
import io.tofhir.server.service.SimpleStructureDefinitionService
import io.tofhir.server.service.schema.SchemaFolderRepository.SCHEMAS_JSON
import io.tofhir.server.util.FileOperations

import java.io.{File, FileWriter}
import java.nio.charset.StandardCharsets
import scala.collection.mutable
import scala.concurrent.Future
import scala.io.Source

/**
 * Folder/Directory based schema repository implementation.
 *
 * @param schemaRepositoryFolderPath
 */
class SchemaFolderRepository(schemaRepositoryFolderPath: String) extends AbstractSchemaRepository {

  private val fhirConfigReader: IFhirConfigReader = new FSConfigReader(profilesPath = Some(schemaRepositoryFolderPath))

  // BaseFhirConfig will act as a cache by holding the ProfileDefinitions in memory
  private val baseFhirConfig: BaseFhirConfig = fhirConfigurator.initializePlatform(fhirConfigReader)
  private var simpleStructureDefinitionService = new SimpleStructureDefinitionService(baseFhirConfig)

  private val schemaDefinitions: mutable.Map[String, SchemaDefinition] = initMap(schemaRepositoryFolderPath)



  /**
   * Retrieve the metadata of all SchemaDefinitions (only url, type and name fields are populated)
   *
   * @return
   */
  override def getAllSchemaMetadata(projectId: String): Future[Seq[SchemaDefinition]] = {
    Future {
      schemaDefinitions.values.filter(schema => schema.project.equals(projectId)).map(schema => SchemaDefinition(schema.id, schema.url, schema.`type`, schema.name, projectId, None, None)).toSeq
    }
  }

  /**
   * Retrieve the schema identified by its id.
   *
   * @param id
   * @return
   */
  override def getSchema(id: String): Future[Option[SchemaDefinition]] = {
    Future {
      schemaDefinitions.get(id)
    }
  }

  /**
   * Save the schema to the repository.
   *
   * @param schemaDefinition
   * @return
   */
  override def saveSchema(schemaDefinition: SchemaDefinition): Future[SchemaDefinition] = {
    Future {
      // Validate
      val structureDefinitionResource = convertToStructureDefinitionResource(schemaDefinition)
      try {
        fhirConfigurator.validateGivenInfrastructureResources(baseFhirConfig, api.FHIR_FOUNDATION_RESOURCES.FHIR_STRUCTURE_DEFINITION, Seq(structureDefinitionResource))
      } catch {
        case e: Exception =>
          throw BadRequest("Schema definition is not valid.", s"Schema definition cannot be validated: ${schemaDefinition.url}", Some(e))
      }

      if (schemaDefinitions.contains(schemaDefinition.id)) {
        throw AlreadyExists("Schema already exists.", s"A schema definition with id ${schemaDefinition.id} already exists in the schema repository at ${FileUtils.getPath(schemaRepositoryFolderPath).toAbsolutePath.toString}")
      }

      // Write to the repository as a new file
      val newFile = getFileForSchema(schemaDefinition)
      val fw = new FileWriter(newFile)
      fw.write(structureDefinitionResource.toPrettyJson)
      fw.close()

      // Update the caches with the new schema
      baseFhirConfig.profileRestrictions += schemaDefinition.url -> fhirFoundationResourceParser.parseStructureDefinition(structureDefinitionResource, includeElementMetadata = true)
      schemaDefinitions.put(schemaDefinition.id, schemaDefinition)

      // Update metadata file by adding the new schema
      FileOperations.updateMetadata[SchemaDefinition](FileUtils.getParentFilePath(schemaRepositoryFolderPath), SCHEMAS_JSON, schemaDefinitions.values.map(getSchemaMetadata).toSeq.sortWith(schemaComparisonFunc))

      schemaDefinition
    }
  }

  /**
   * Update the schema in the repository.
   * @param id Type of the schema
   * @param schemaDefinition Schema definition
   * @return
   */
  override def putSchema(id: String, schemaDefinition: SchemaDefinition): Future[Unit] = {
    if (!schemaDefinitions.contains(schemaDefinition.id)) {
      throw ResourceNotFound("Schema does not exists.", s"A schema definition with id ${schemaDefinition.id} does not exists in the schema repository at ${FileUtils.getPath(schemaRepositoryFolderPath).toAbsolutePath.toString}")
    }

    Future {
      // Validate
      val structureDefinitionResource = convertToStructureDefinitionResource(schemaDefinition)
      try {
        fhirConfigurator.validateGivenInfrastructureResources(baseFhirConfig, api.FHIR_FOUNDATION_RESOURCES.FHIR_STRUCTURE_DEFINITION, Seq(structureDefinitionResource))
      } catch {
        case e: Exception =>
          throw BadRequest("Schema definition is not valid.", s"Schema definition cannot be validated: ${schemaDefinition.url}", Some(e))
      }

      // Check explicit id and schema definition id are the same
      if (!id.equals(schemaDefinition.id)) {
        throw BadRequest("Schema definition is not valid.", s"Identifier of the schema definition: ${schemaDefinition.id} does not match with explicit id: $id")
      }

      // If the name of the schemas are different remove the old file
      if (!schemaDefinitions(id).name.equals(schemaDefinition.name)) {
        val oldFile: File = getFileForSchema(schemaDefinitions(id))
        oldFile.delete()
      }

      // Update the file
      val file = getFileForSchema(schemaDefinition)
      val fw = new FileWriter(file)
      import io.onfhir.util.JsonFormatter._
      fw.write(structureDefinitionResource.toPrettyJson)
      fw.close()

      // Update cache
      baseFhirConfig.profileRestrictions += schemaDefinition.url -> fhirFoundationResourceParser.parseStructureDefinition(structureDefinitionResource, includeElementMetadata = true)
      schemaDefinitions.put(id, schemaDefinition)

      // Update the metadata file
      val remainingSchemas: Seq[SchemaDefinition] = schemaDefinitions.values.filterNot(schema => schema.id.equals(id)).toSeq.sortWith(schemaComparisonFunc)
      FileOperations.updateMetadata[SchemaDefinition](FileUtils.getParentFilePath(schemaRepositoryFolderPath), SCHEMAS_JSON, remainingSchemas.map(getSchemaMetadata) :+ getSchemaMetadata(schemaDefinition))
    }
  }

  /**
   * Delete the schema from the repository.
   * @param id Name of the schema
   * @return
   */
  override def deleteSchema(id: String): Future[Unit] = {
    if (schemaDefinitions.contains(id)) {
      throw AlreadyExists("Schema does not exists.", s"A schema definition with id ${id} does not exists in the schema repository at ${FileUtils.getPath(schemaRepositoryFolderPath).toAbsolutePath.toString}")
    }

    Future {
      // Update cache
      val schema: SchemaDefinition = schemaDefinitions(id)
      schemaDefinitions.remove(id)
      baseFhirConfig.profileRestrictions -= schema.url

      val fileName = getFileName(id, schemaDefinitions(id).name)
      val file = FileUtils.findFileByName(schemaRepositoryFolderPath, fileName)
      file.get.delete()
    }
  }

  /**
   * Gets the file for the given schema definition.
   *
   * @param schemaDefinition
   * @return
   */
  private def getFileForSchema(schemaDefinition: SchemaDefinition): File = {
    // First construct a path by concatenating the repository path, project name and file name
    val file: File = FileUtils.getPath(schemaRepositoryFolderPath, schemaDefinition.project, getFileName(schemaDefinition.id, schemaDefinition.name)).toFile
    // If the project folder does not exist, create it
    if (!file.getParentFile.exists()) {
      file.getParentFile.mkdir()
    }
    file
  }

  /**
   * Constructs the file name for the schema file given the id and name
   *
   * @param schemaId
   * @param schemaName
   * @return
   */
  private def getFileName(schemaId: String, schemaName: String): String = {
    s"${FileUtils.getFileName(schemaId, schemaName)}${FileExtensions.StructureDefinition}${FileExtensions.JSON}"
  }

  /**
   * Copies the given SchemaDefinition with only the metadata attributes
   *
   * @param schema
   * @return
   */
  private def getSchemaMetadata(schema: SchemaDefinition): SchemaDefinition = {
    schema.copy(id = schema.id, url = schema.url, `type` = schema.`type`, name = schema.name, project = schema.project)
  }

  /**
   * Parses the given schema folder and creates a SchemaDefinition map
   *
   * @param schemaRepositoryFolderPath
   * @return
   */
  private def initMap(schemaRepositoryFolderPath: String): mutable.Map[String, SchemaDefinition] = {
    val schemaDefinitionMap = mutable.Map[String, SchemaDefinition]()
    val folder = new File(schemaRepositoryFolderPath)
    var files = Seq.empty[File]
    try {
      files = IOUtil.getFilesFromFolder(folder, withExtension = Some(FileExtensions.JSON.toString), recursively = Some(true))
    } catch {
      case e: Throwable => throw FhirMappingException(s"Given folder for the mapping repository is not valid.", e)
    }

    // Read each file containing ProfileRestrictions and convert them to SchemaDefinitions
    files.map { f =>
      val source = Source.fromFile(f, StandardCharsets.UTF_8.name()) // read the JSON file
      val fileContent = try source.mkString finally source.close()
      val structureDefinition: ProfileRestrictions = fhirFoundationResourceParser.parseStructureDefinition(fileContent.parseJson)
      val schemaId: String = FileUtils.getId(f.getName)
      val project: String = f.getParentFile.getName
      val schema = convertToSchemaDefinition(structureDefinition, schemaId, project, simpleStructureDefinitionService)
      schemaDefinitionMap.put(schema.id, schema)
    }
    schemaDefinitionMap
  }

  /**
   * Comparison function for two SchemaDefinitions. The definitions are compared according to their names
   * @param s1
   * @param s2
   * @return
   */
  private def schemaComparisonFunc(s1: SchemaDefinition, s2: SchemaDefinition): Boolean = {
    s1.name.compareTo(s2.name) < 0
  }
}

/**
 * Keeps file/folder names related to the project repository
 * */
object SchemaFolderRepository {
  val SCHEMAS_JSON = "schemas.json" // file keeping the metadata of all schemas
}



