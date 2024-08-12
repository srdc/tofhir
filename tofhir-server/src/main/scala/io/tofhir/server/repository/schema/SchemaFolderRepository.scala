package io.tofhir.server.repository.schema

import com.typesafe.scalalogging.Logger
import io.onfhir.api
import io.onfhir.api.{FHIR_FOUNDATION_RESOURCES, FHIR_ROOT_URL_FOR_DEFINITIONS, Resource}
import io.onfhir.api.util.IOUtil
import io.onfhir.api.validation.ProfileRestrictions
import io.onfhir.config.{BaseFhirConfig, FSConfigReader, IFhirConfigReader}
import io.onfhir.exception.InitializationException
import io.onfhir.util.JsonFormatter._
import io.tofhir.common.model.SchemaDefinition
import io.tofhir.common.util.SchemaUtil
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.config.ToFhirConfig
import io.tofhir.engine.mapping.schema.SchemaConverter
import io.tofhir.engine.model.exception.FhirMappingException
import io.tofhir.engine.util.FileUtils.FileExtensions
import io.tofhir.engine.util.{FhirVersionUtil, FileUtils}
import io.tofhir.server.common.model.{AlreadyExists, BadRequest, ResourceNotFound}
import io.tofhir.server.model.Project
import io.tofhir.server.repository.project.ProjectFolderRepository
import io.tofhir.server.service.fhir.SimpleStructureDefinitionService
import org.apache.spark.sql.types.StructType

import java.io.{File, FileWriter}
import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.collection.mutable
import scala.concurrent.Future
import scala.io.Source
import scala.language.postfixOps

/**
 * Folder/Directory based schema repository implementation.
 *
 * @param schemaRepositoryFolderPath
 * @param projectFolderRepository
 */
class SchemaFolderRepository(schemaRepositoryFolderPath: String, projectFolderRepository: ProjectFolderRepository) extends AbstractSchemaRepository {

  private val logger: Logger = Logger(this.getClass)

  private val fhirConfigReader: IFhirConfigReader = new FSConfigReader(
    fhirVersion = FhirVersionUtil.getMajorFhirVersion(ToFhirConfig.engineConfig.schemaRepositoryFhirVersion),
    profilesPath = Some(FileUtils.getPath(schemaRepositoryFolderPath).toString))
  // BaseFhirConfig will act as a cache by holding the ProfileDefinitions in memory
  private val baseFhirConfig: BaseFhirConfig = initBaseFhirConfig(fhirConfigReader)
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
        structureDefinitionResource = SchemaUtil.convertToStructureDefinitionResource(schemaDefinition, ToFhirConfig.engineConfig.schemaRepositoryFhirVersion)
      } catch {
        case _: IllegalArgumentException => throw BadRequest("Missing data type.", s"A field definition must have at least one data type. Element rootPath: ${schemaDefinition.`type`}")
      }
      try {
        fhirConfigurator.validateGivenInfrastructureResources(baseFhirConfig, api.FHIR_FOUNDATION_RESOURCES.FHIR_STRUCTURE_DEFINITION, Seq(structureDefinitionResource))
      } catch {
        case e: Exception =>
          throw BadRequest("Schema definition is not valid.", s"Schema definition cannot be validated: ${schemaDefinition.url}", Some(e))
      }

      checkIfSchemaIsUnique(projectId, schemaDefinition.id, schemaDefinition.url)

    // Check SchemaDefinition type is valid
    this.validateSchemaDefinitionType(schemaDefinition);

    // Write to the repository as a new file and update caches
    writeSchemaAndUpdateCaches(projectId, structureDefinitionResource, schemaDefinition)
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
      structureDefinitionResource = SchemaUtil.convertToStructureDefinitionResource(schemaDefinition, ToFhirConfig.engineConfig.schemaRepositoryFhirVersion)
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
        getFileForSchema(projectId, oldSchema.id).map(oldFile => {
          oldFile.delete()
        })
      } else {
        Future.apply()
      }
    }.flatMap(_ => {
      // Update the file
      getFileForSchema(projectId, schemaDefinition.id).map(file => {

        val fw = new FileWriter(file)
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
    if (!schemaDefinitions(projectId).contains(id)) {
      throw ResourceNotFound("Schema does not exists.", s"A schema with id $id does not exists in the schema repository at ${FileUtils.getPath(schemaRepositoryFolderPath).toAbsolutePath.toString}")
    }

    // delete schema file from repository
    getFileForSchema(projectId, id).map(file => {
      file.delete()
      val schema: SchemaDefinition = schemaDefinitions(projectId)(id)
      // delete the schema from the in-memory map
      schemaDefinitions(projectId).remove(id)
      // remove the url of the schema
      baseFhirConfig.profileRestrictions -= schema.url
      // Update project
      projectFolderRepository.deleteSchema(projectId, Some(id))
    })
  }

  /**
   * Deletes all schemas associated with a specific project.
   *
   * @param projectId The unique identifier of the project for which schemas should be deleted.
   */
  override def deleteProjectSchemas(projectId: String): Unit = {
    // delete schema definitions for the project
    org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(schemaRepositoryFolderPath, projectId).toFile)
    // remove profile restrictions of project schemas
    val schemaUrls:Set[String] = schemaDefinitions.getOrElse(projectId, mutable.Map.empty)
      .values.map(definition => definition.url)
      .toSet
    baseFhirConfig.profileRestrictions --= schemaUrls
    // remove project from the cache
    schemaDefinitions.remove(projectId)
    // delete project schemas
    projectFolderRepository.deleteSchema(projectId)
  }

  /**
   * Gets the file for the given schema definition.
   *
   * @param schemaDefinition
   * @return
   */
  private def getFileForSchema(projectId: String, id: String): Future[File] = {
    val projectFuture: Future[Option[Project]] = projectFolderRepository.getProject(projectId)
    projectFuture.map(project => {
      val file: File = FileUtils.getPath(schemaRepositoryFolderPath, project.get.id, getFileName(id)).toFile
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
        val decomposedSchema: Resource = SchemaUtil.convertToStructureDefinitionResource(s, ToFhirConfig.engineConfig.schemaRepositoryFhirVersion) // Schema definition in the FHIR Resource representation
        new SchemaConverter(FhirVersionUtil.getMajorFhirVersion(ToFhirConfig.engineConfig.schemaRepositoryFhirVersion)).convertSchema(decomposedSchema)
      })
  }

  /**
   * Try to initialize the BaseFhirConfig. Otherwise print the error message and halt.
   * @param fhirConfigReader config reader for BaseFhirConfig
   * @return
   */
  private def initBaseFhirConfig(fhirConfigReader: IFhirConfigReader): BaseFhirConfig = {
    val folder = FileUtils.getPath(schemaRepositoryFolderPath).toFile
    if (!folder.exists()) {
      folder.mkdirs()
    }
    var baseFhirConfig: BaseFhirConfig = null
    try {
      baseFhirConfig = fhirConfigurator.initializePlatform(fhirConfigReader)
    } catch {
      case error: InitializationException =>
        logger.error(error.getMessage)
        System.exit(1)
    }
    baseFhirConfig
  }

  /**
   * Retrieve the Structure Definition of the schema identified by its id.
   *
   * @param projectId Project containing the schema definition
   * @param id        Identifier of the schema definition
   * @return          Structure definition of the schema converted into StructureDefinition Resource
   */
  override def getSchemaAsStructureDefinition(projectId: String, id: String): Future[Option[Resource]] = {
    getSchema(projectId, id).map {
      case Some(schemaStructureDefinition) =>
        Some(SchemaUtil.convertToStructureDefinitionResource(schemaStructureDefinition, ToFhirConfig.engineConfig.schemaRepositoryFhirVersion))
      case None =>
        None
    }
  }

  /**
   * Saves the schemas by using their Structure Definition resources.
   *
   * This method validates the given structure definition resources, processes them to create schema definitions,
   * and saves these definitions to the repository. It also ensures that the schema definitions are unique within
   * the specified project and updates the cache accordingly.
   *
   * Throws:
   *  - BadRequest: If the schema resource cannot be validated.
   *
   * @param projectId The identifier of the project in which the schemas will be created.
   * @param structureDefinitionResources A sequence of structure definition resources for the schemas.
   * @return A Future containing a sequence of SchemaDefinition objects representing the created schemas.
   */
  override def saveSchemaByStructureDefinition(projectId: String, structureDefinitionResources: Seq[Resource]): Future[Seq[SchemaDefinition]] = {
    // Extract the URLs of the schemas that are about to be saved. These URLs will be used later to validate
    // any referenced schemas within the current schema, ensuring they exist either as base FHIR definitions,
    // already present schemas, or as part of this batch.
    val schemaUrls = structureDefinitionResources.map(resource => (resource \ "url").extract[String])
    // convert each Resource to a SchemaDefinition
    val schemaDefinitions: Seq[SchemaDefinition] = structureDefinitionResources.map(structureDefinitionResource => {
      // Validate the resource
      try {
        fhirConfigurator.validateGivenInfrastructureResources(baseFhirConfig, api.FHIR_FOUNDATION_RESOURCES.FHIR_STRUCTURE_DEFINITION, Seq(structureDefinitionResource))
      } catch {
        case e: Exception =>
          throw BadRequest("Schema resource is not valid.", s"Schema resource cannot be validated.", Some(e))
      }
      // Validate that all referenced schemas in the current schema exist or are part of the schemas to be saved.
      validateReferencedSchemas(projectId, structureDefinitionResource, schemaUrls)

      // Create structureDefinition from the resource
      val structureDefinition: ProfileRestrictions = fhirFoundationResourceParser.parseStructureDefinition(structureDefinitionResource, includeElementMetadata = true)
      // Generate an Id if id is missing
      val schemaId = structureDefinition.id.getOrElse(UUID.randomUUID().toString)

      checkIfSchemaIsUnique(projectId, schemaId, structureDefinition.url)

      // To use convertToSchemaDefinition, profileRestrictions sequence must include the structure definition. Add it before conversion
      baseFhirConfig.profileRestrictions += structureDefinition.url -> structureDefinition
      val schemaDefinition = convertToSchemaDefinition(structureDefinition, simpleStructureDefinitionService)
      // Remove structure definition from the cache and add it after file writing is done to ensure files and cache are the same
      baseFhirConfig.profileRestrictions -= structureDefinition.url

      // Check SchemaDefinition type is valid.
      this.validateSchemaDefinitionType(schemaDefinition)
      schemaDefinition
    })
    // write the schemas to the repository as a new file and update caches
    val futures: Seq[Future[SchemaDefinition]] = schemaDefinitions.zipWithIndex.map {
      case (schemaDefinition, index) =>
        writeSchemaAndUpdateCaches(projectId, structureDefinitionResources.lift(index).get, schemaDefinition)
    }
    Future.sequence(futures)
  }

  /**
   * Checks if ID of the schema is unique in the project and url is unique in the program
   * Throws:
   *  {@link AlreadyExists} with the code 409 if the schema id is not unique in the project or the schema url is not unique in the program
   *
   * @param projectId Identifier of the project to check schema ID's in it
   * @param schemaId Identifier of the schema
   * @param schemaUrl Url of the schema
   */
  private def checkIfSchemaIsUnique(projectId: String, schemaId: String, schemaUrl: String): Unit = {
    if (schemaDefinitions.contains(projectId) && schemaDefinitions(projectId).contains(schemaId)) {
      throw AlreadyExists("Schema already exists.", s"A schema definition with id ${schemaId} already exists in the schema repository at ${FileUtils.getPath(schemaRepositoryFolderPath).toAbsolutePath.toString}")
    }

    val schemaUrls: Map[String, String] = schemaDefinitions.values.flatMap(_.values).map(schema => schema.url -> schema.name).toMap
    if (schemaUrls.contains(schemaUrl)) {
      throw AlreadyExists("Schema already exists.", s"A schema definition with url ${schemaUrl} already exists. Check the schema '${schemaUrls(schemaUrl)}'")
    }
  }

  /**
   * Write the schema file and update the caches accordingly
   * @param projectId Id of the project that will include the schema
   * @param structureDefinitionResource Schema resource that will be written
   * @param schemaDefinition Definition of the schema
   * @return
   */
  private def writeSchemaAndUpdateCaches(projectId: String, structureDefinitionResource: Resource, schemaDefinition: SchemaDefinition ): Future[SchemaDefinition] = {
    getFileForSchema(projectId, schemaDefinition.id).map(newFile => {
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
   * Check SchemaDefinition type starts with uppercase.
   * @param schemaDefinition Definition of the schema
   * @return
   */
  private def validateSchemaDefinitionType(schemaDefinition: SchemaDefinition): Unit = {
    val schemaDefinitionType: String = schemaDefinition.`type`;
    if(schemaDefinitionType.isEmpty || schemaDefinitionType.apply(0).isLower){
      throw BadRequest("Schema definition is not valid.", s"Schema definition type must starts with uppercase!");
    }
  }

  /**
   * Validates that all referenced profiles in a given schema either exist as base FHIR definitions,
   * are already present in the system, or are included in the list of schemas to be created.
   *
   * @param projectId The ID of the project in which the schemas are being validated.
   * @param schemaResource The FHIR StructureDefinition resource representing the schema to be validated.
   * @param schemaUrls The list of URLs of the schemas that are included in the current batch to be created.
   * @throws BadRequest if any referenced profile is missing.
   */
  private def validateReferencedSchemas(projectId: String, schemaResource: Resource, schemaUrls: Seq[String]): Unit = {
    /**
     * Validates a single profile URL to ensure it either exists as a base FHIR definition,
     * is already present in the system, or is included in the provided schema URLs list.
     *
     * @param profile   The URL of the profile to be validated.
     * @param schemaUrl The URL of the schema that references this profile, used for error reporting.
     * @throws BadRequest if the profile is missing.
     */
    def validateProfile(profile: String, schemaUrl: String): Unit = {
      if (!profile.startsWith(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/${FHIR_FOUNDATION_RESOURCES.FHIR_STRUCTURE_DEFINITION}") &&
        !schemaUrls.contains(profile) &&
        !schemaDefinitions(projectId).values.exists(s => s.url.contentEquals(profile))) {
        throw BadRequest("Invalid Schema Reference !", s"The schema with URL '$schemaUrl' references a non-existent schema: '$profile'. Ensure all referenced schemas exist.")
      }
    }

    // Validate the base definition of the schema
    val baseDefinitionUrl: String = (schemaResource \ "baseDefinition").extract[String]
    val schemaUrl: String = (schemaResource \ "url").extract[String]
    validateProfile(baseDefinitionUrl, schemaUrl)

    // Validate the profiles associated with each element in the schema
    val elements = (schemaResource \ "differential" \ "element").extract[Seq[Resource]]
    elements.foreach { element =>
      val types = (element \ "type").extract[Seq[Resource]]
      types.foreach { elementType =>
        val profiles = (elementType \ "profile").extract[Seq[String]]
        val targetProfiles = (elementType \ "targetProfile").extract[Seq[String]]
        val allProfiles = profiles ++ targetProfiles
        allProfiles.foreach(profile => validateProfile(profile, schemaUrl))
      }
    }
  }
}

