package io.tofhir.server.service.schema

import io.onfhir.api
import io.onfhir.api.Resource
import io.onfhir.api.util.IOUtil
import io.onfhir.api.validation.ProfileRestrictions
import io.onfhir.config.{BaseFhirConfig, FSConfigReader, IFhirConfigReader}
import io.onfhir.util.JsonFormatter._
import io.tofhir.common.model.SchemaDefinition
import io.tofhir.common.util.SchemaUtil
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.mapping.{AbstractFhirSchemaLoader, SchemaConverter}
import io.tofhir.engine.model.FhirMappingException
import io.tofhir.engine.util.FileUtils
import io.tofhir.engine.util.FileUtils.FileExtensions
import io.tofhir.server.model.{AlreadyExists, BadRequest, Project, ResourceNotFound}
import io.tofhir.server.service.SimpleStructureDefinitionService
import io.tofhir.server.service.project.ProjectFolderRepository
import org.apache.spark.sql.types.StructType
import org.json4s.{Extraction, JBool, JObject}

import java.io.{File, FileWriter}
import java.nio.charset.StandardCharsets
import scala.collection.mutable
import scala.concurrent.Future
import scala.io.Source
import scala.util.Try

/**
 * Folder/Directory based schema repository implementation.
 *
 * @param schemaRepositoryFolderPath
 * @param projectFolderRepository
 */
class SchemaFolderRepository(schemaRepositoryFolderPath: String, projectFolderRepository: ProjectFolderRepository) extends AbstractSchemaRepository {

  private val fhirConfigReader: IFhirConfigReader = new FSConfigReader(
    profilesPath = Some(FileUtils.getPath(schemaRepositoryFolderPath).toString))
  // BaseFhirConfig will act as a cache by holding the ProfileDefinitions in memory
  private val baseFhirConfig: BaseFhirConfig = fhirConfigurator.initializePlatform(fhirConfigReader)
  private val simpleStructureDefinitionService = new SimpleStructureDefinitionService(baseFhirConfig)
  // Schema definition cache: project id -> schema id -> schema definition
  private val schemaDefinitions: mutable.Map[String, mutable.Map[String, SchemaDefinition]] = initMap(schemaRepositoryFolderPath)

  /**
   * Returns the schema cached schema definitions by this repository
   *
   * @return
   */
  def getCachedSchemas(): mutable.Map[String, mutable.Map[String, SchemaDefinition]] = {
    schemaDefinitions
  }

  /**
   * Retrieve all SchemaDefinitions
   *
   * @return
   */
  override def getAllSchemas(projectId: String): Future[Seq[SchemaDefinition]] = {
    Future {
      if (schemaDefinitions.contains(projectId)) {
        schemaDefinitions(projectId).values.toSeq.sortWith(schemaComparisonFunc)
      } else {
        Seq.empty
      }
    }
  }

  /**
   * Retrieve the schema identified by its project and id.
   *
   * @param id
   * @return
   */
  override def getSchema(projectId: String, id: String): Future[Option[SchemaDefinition]] = {
    Future {
      schemaDefinitions(projectId).get(id)
    }
  }

  /**
   * Retrieve the schema identified by its url.
   *
   * @param projectId Project containing the schema definition
   * @param url       URL of the schema definition
   * @return
   */
  override def getSchemaByUrl(projectId: String, url: String): Future[Option[SchemaDefinition]] = {
    Future {
      schemaDefinitions(projectId).values.find(_.url.equals(url))
    }
  }

  /**
   * Save the schema to the repository.
   *
   * @param schemaDefinition
   * @return
   */
  override def saveSchema(projectId: String, schemaDefinition: SchemaDefinition): Future[SchemaDefinition] = {
      // Validate
      var structureDefinitionResource:Resource = null
      try{
        structureDefinitionResource = SchemaUtil.convertToStructureDefinitionResource(schemaDefinition)
      } catch {
        case _: IllegalArgumentException => throw BadRequest("Missing data type.", s"A field definition must have at least one data type. Element rootPath: ${schemaDefinition.`type`}")
      }
      try {
        fhirConfigurator.validateGivenInfrastructureResources(baseFhirConfig, api.FHIR_FOUNDATION_RESOURCES.FHIR_STRUCTURE_DEFINITION, Seq(structureDefinitionResource))
      } catch {
        case e: Exception =>
          throw BadRequest("Schema definition is not valid.", s"Schema definition cannot be validated: ${schemaDefinition.url}", Some(e))
      }

      if (schemaDefinitions.contains(projectId) && schemaDefinitions(projectId).contains(schemaDefinition.id)) {
        throw AlreadyExists("Schema already exists.", s"A schema definition with id ${schemaDefinition.id} already exists in the schema repository at ${FileUtils.getPath(schemaRepositoryFolderPath).toAbsolutePath.toString}")
      }

      // Write to the repository as a new file
      getFileForSchema(projectId, schemaDefinition).map(newFile => {
        val fw = new FileWriter(newFile)
        fw.write(structureDefinitionResource.toPrettyJson)
        fw.close()

        // Update the project with the schema
        projectFolderRepository.addSchema(projectId, schemaDefinition)

        // Update the caches with the new schema
        baseFhirConfig.profileRestrictions += schemaDefinition.url -> fhirFoundationResourceParser.parseStructureDefinition(structureDefinitionResource, includeElementMetadata = true)
        schemaDefinitions.getOrElseUpdate(projectId, mutable.Map.empty).put(schemaDefinition.id, schemaDefinition)

        schemaDefinition
      })
  }

  /**
   * Update the schema in the repository.
   *
   * @param projectId        Project containing the schema definition
   * @param id               Type of the schema
   * @param schemaDefinition Schema definition
   * @return
   */
  override def updateSchema(projectId: String, id: String, schemaDefinition: SchemaDefinition): Future[Unit] = {
    if (!schemaDefinitions.contains(projectId) || !schemaDefinitions(projectId).contains(schemaDefinition.id) || !schemaDefinitions(projectId).contains(id)) {
      throw ResourceNotFound("Schema does not exists.", s"A schema definition with id ${schemaDefinition.id} does not exists in the schema repository at ${FileUtils.getPath(schemaRepositoryFolderPath).toAbsolutePath.toString}")
    }

    // Validate
    var structureDefinitionResource:Resource = null
    try{
      structureDefinitionResource = SchemaUtil.convertToStructureDefinitionResource(schemaDefinition)
    } catch {
      case _: IllegalArgumentException => throw BadRequest("Missing data type.", s"A field definition must have at least one data type. Element rootPath: ${schemaDefinition.`type`}")
    }
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
    {
      val oldSchema: SchemaDefinition = schemaDefinitions(projectId)(id)
      if (!oldSchema.name.equals(schemaDefinition.name)) {
        getFileForSchema(projectId, oldSchema).map(oldFile => {
          oldFile.delete()
        })
      } else {
        Future.apply()
      }
    }.flatMap(_ => {
      // Update the file
      getFileForSchema(projectId, schemaDefinition).map(file => {

        val fw = new FileWriter(file)
        import io.onfhir.util.JsonFormatter._
        fw.write(structureDefinitionResource.toPrettyJson)
        fw.close()

        // Update cache
        baseFhirConfig.profileRestrictions += schemaDefinition.url -> fhirFoundationResourceParser.parseStructureDefinition(structureDefinitionResource, includeElementMetadata = true)
        schemaDefinitions(projectId).put(id, schemaDefinition)

        // Update the project
        projectFolderRepository.updateSchema(projectId, schemaDefinition)
      })
    })
  }

  /**
   * Delete the schema from the repository.
   * @param id Name of the schema
   * @return
   */
  override def deleteSchema(projectId: String, id: String): Future[Unit] = {
    if (!schemaDefinitions.contains(projectId) || !schemaDefinitions(projectId).contains(id)) {
      throw ResourceNotFound("Schema does not exists.", s"A schema definition with id ${id} does not exists in the schema repository at ${FileUtils.getPath(schemaRepositoryFolderPath).toAbsolutePath.toString}")
    }

    Future {
      // Update cache
      val schema: SchemaDefinition = schemaDefinitions(projectId)(id)
      schemaDefinitions(projectId).remove(id)
      baseFhirConfig.profileRestrictions -= schema.url

      val fileName = getFileName(id)
      val file = FileUtils.findFileByName(schemaRepositoryFolderPath, fileName)
      file.get.delete()

      // Update project
      projectFolderRepository.deleteSchema(projectId, schema.id)
    }
  }

  /**
   * Gets the file for the given schema definition.
   *
   * @param schemaDefinition
   * @return
   */
  private def getFileForSchema(projectId: String, schemaDefinition: SchemaDefinition): Future[File] = {
    val projectFuture: Future[Option[Project]] = projectFolderRepository.getProject(projectId)
    projectFuture.map(project => {
      val file: File = FileUtils.getPath(schemaRepositoryFolderPath, project.get.id, getFileName(schemaDefinition.id)).toFile
      // If the project folder does not exist, create it
      if (!file.getParentFile.exists()) {
        file.getParentFile.mkdir()
      }
      file
    })
  }

  /**
   * Constructs the file name for the schema file given the id
   *
   * @param schemaId
   * @return
   */
  private def getFileName(schemaId: String): String = {
    s"$schemaId${FileExtensions.StructureDefinition}${FileExtensions.JSON}"
  }

  /**
   * Parses the given schema folder and creates a SchemaDefinition map
   *
   * @param schemaRepositoryFolderPath
   * @return
   */
  private def initMap(schemaRepositoryFolderPath: String): mutable.Map[String, mutable.Map[String, SchemaDefinition]] = {
    val schemaDefinitionMap = mutable.Map[String, mutable.Map[String, SchemaDefinition]]()
    val folder = FileUtils.getPath(schemaRepositoryFolderPath).toFile
    if (!folder.exists()) {
      folder.mkdirs()
    }
    folder.listFiles().foreach(projectFolder => {
      var files = Seq.empty[File]
      try {
        files = IOUtil.getFilesFromFolder(projectFolder, withExtension = Some(FileExtensions.JSON.toString), recursively = Some(true))
      } catch {
        case e: Throwable => throw FhirMappingException(s"Given folder for the mapping repository is not valid.", e)
      }

      // Read each file containing ProfileRestrictions and convert them to SchemaDefinitions
      val projectSchemas: mutable.Map[String, SchemaDefinition] = mutable.Map.empty
      files.map { f =>
        val source = Source.fromFile(f, StandardCharsets.UTF_8.name()) // read the JSON file
        val fileContent = try source.mkString finally source.close()
        val structureDefinition: ProfileRestrictions = fhirFoundationResourceParser.parseStructureDefinition(fileContent.parseJson)
        val schema = convertToSchemaDefinition(structureDefinition, simpleStructureDefinitionService)
        projectSchemas.put(schema.id, schema)
      }

      schemaDefinitionMap.put(projectFolder.getName, projectSchemas)
    })

    schemaDefinitionMap
  }

  /**
   * Comparison function for two SchemaDefinitions. The definitions are compared according to their names
   *
   * @param s1
   * @param s2
   * @return
   */
  private def schemaComparisonFunc(s1: SchemaDefinition, s2: SchemaDefinition): Boolean = {
    s1.name.compareTo(s2.name) < 0
  }

  /**
   * Read the schema given with the url and convert it to the Spark schema
   *
   * @param schemaUrl URL of the schema
   * @return
   */
  override def getSchema(schemaUrl: String): Option[StructType] = {
    schemaDefinitions.values
      .flatMap(_.values) // Flatten all the schemas managed for all projects
      .find(_.url.contentEquals(schemaUrl)) // Find the desired url
      .map(s => {
        val decomposedSchema: Resource = SchemaUtil.convertToStructureDefinitionResource(s) // Schema definition in the FHIR Resource representation
        new SchemaConverter(fhirVersion).convertSchema(decomposedSchema)
      })
  }
}

