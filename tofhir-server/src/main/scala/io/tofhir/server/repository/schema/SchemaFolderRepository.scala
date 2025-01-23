package io.tofhir.server.repository.schema

import com.typesafe.scalalogging.Logger
import io.onfhir.api
import io.onfhir.api.util.IOUtil
import io.onfhir.api.validation.ProfileRestrictions
import io.onfhir.api.{FHIR_FOUNDATION_RESOURCES, FHIR_ROOT_URL_FOR_DEFINITIONS, Resource}
import io.onfhir.config.{BaseFhirConfig, FSConfigReader, IFhirConfigReader}
import io.onfhir.definitions.common.model.SchemaDefinition
import io.onfhir.definitions.common.util.HashUtil
import io.onfhir.definitions.resource.service.SimpleStructureDefinitionService
import io.onfhir.exception.InitializationException
import io.onfhir.util.JsonFormatter._
import io.tofhir.common.util.SchemaUtil
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.config.ToFhirConfig
import io.tofhir.engine.mapping.schema.SchemaConverter
import io.tofhir.engine.model.exception.FhirMappingException
import io.tofhir.engine.util.FileUtils.FileExtensions
import io.tofhir.engine.util.{FhirVersionUtil, FileUtils}
import io.tofhir.server.common.model.{AlreadyExists, BadRequest, ResourceNotFound}
import io.tofhir.server.repository.project.IProjectRepository
import io.tofhir.server.util.FileOperations
import org.apache.spark.sql.types.StructType

import java.io.{File, FileWriter}
import java.nio.charset.StandardCharsets
import scala.collection.mutable
import scala.concurrent.Future
import scala.io.Source
import scala.language.postfixOps

/**
 * Folder/Directory based schema repository implementation.
 *
 * @param schemaRepositoryFolderPath
 * @param projectRepository
 */
class SchemaFolderRepository(schemaRepositoryFolderPath: String, projectRepository: IProjectRepository) extends AbstractSchemaRepository {

  private val logger: Logger = Logger(this.getClass)
  private var baseFhirConfig: BaseFhirConfig = initBaseFhirConfig()
  private var simpleStructureDefinitionService = new SimpleStructureDefinitionService(baseFhirConfig)

  // Schema definition cache: project id -> schema id -> schema definition
  private val schemaDefinitions: mutable.Map[String, mutable.Map[String, SchemaDefinition]] = mutable.Map.empty[String, mutable.Map[String, SchemaDefinition]]
  // Initialize the map for the first time
  initMap(schemaRepositoryFolderPath)

  /**
   * Retrieve all SchemaDefinitions
   *
   * @return
   */
  override def getAllSchemas(projectId: String): Future[Seq[SchemaDefinition]] = {
    Future {
      schemaDefinitions.get(projectId)
        .map(_.values.toSeq) // If such a project exists, return the schema definitions as a sequence
        .getOrElse(Seq.empty[SchemaDefinition]) // Else, return an empty list
    }
  }

  /**
   * Retrieve the schema identified by its project and id.
   *
   * @param schemaId
   * @return
   */
  override def getSchema(projectId: String, schemaId: String): Future[Option[SchemaDefinition]] = {
    Future {
      schemaDefinitions(projectId).get(schemaId)
    }
  }

  /**
   * Retrieve the schema identified by its url.
   *
   * @param url URL of the schema definition
   * @return
   */
  override def getSchemaByUrl(url: String): Future[Option[SchemaDefinition]] = {
    Future {
      schemaDefinitions.values.flatMap(_.values).find(_.url.equals(url))
    }
  }

  /**
   * Save the schema to the repository.
   *
   * @param schemaDefinition
   * @return
   */
  override def saveSchema(projectId: String, schemaDefinition: SchemaDefinition): Future[SchemaDefinition] = {
    // Validate the given schema
    var structureDefinitionResource: Resource = null
    try {
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

    // Ensure that both the ID and "URL|version" (the canonical URL) of the schema is unique/
    // At this point, also ensure that the name of the schema is unique
    // If id field is not provided within the schema object, the schemaID will be unique
    //  because in its constructor we put the MD5 hash of the URL into the id field for new schema creations.
    checkIfSchemaIsUnique(projectId, schemaDefinition.id, s"${schemaDefinition.url}|${schemaDefinition.version}", Some(schemaDefinition.name))

    // Check SchemaDefinition type is valid (It must start with an uppercase letter)
    validateSchemaDefinitionType(schemaDefinition)

    // Write to the repository as a new file and update caches
    writeSchemaAndUpdateCaches(projectId, structureDefinitionResource, schemaDefinition)
  }

  /**
   * Update the schema in the repository.
   *
   * @param projectId        Project containing the schema definition
   * @param schemaId         Unique ID of the schema
   * @param schemaDefinition Schema definition
   * @return
   */
  override def updateSchema(projectId: String, schemaId: String, schemaDefinition: SchemaDefinition): Future[SchemaDefinition] = {
    if (!schemaDefinitions.contains(projectId) || !schemaDefinitions(projectId).contains(schemaId)) {
      throw ResourceNotFound("Schema does not exists.", s"A schema definition with id $schemaId does not exists in the schema repository at ${FileUtils.getPath(schemaRepositoryFolderPath).toAbsolutePath.toString}")
    }

    // Validate the given schema
    var structureDefinitionResource: Resource = null
    try {
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

    // Update the file
    getFileForSchema(projectId, schemaDefinition.id).flatMap(file => {
      val fw = new FileWriter(file)
      fw.write(structureDefinitionResource.toPrettyJson)
      fw.close()

      // Update cache
      baseFhirConfig.profileRestrictions =
        SchemaManagementUtil.updateProfileRestrictionsMap(baseFhirConfig.profileRestrictions, schemaDefinition.url, schemaDefinition.version, fhirFoundationResourceParser.parseStructureDefinition(structureDefinitionResource, includeElementMetadata = true))
      schemaDefinitions(projectId).put(schemaId, schemaDefinition)

      // Update the project
      projectRepository.updateSchema(projectId, schemaDefinition) map { _ =>
        schemaDefinition
      }
    })

  }

  /**
   * Delete the schema from the repository.
   *
   * @param schemaId Unique ID of the schema
   * @return
   */
  override def deleteSchema(projectId: String, schemaId: String): Future[Unit] = {
    if (!schemaDefinitions(projectId).contains(schemaId)) {
      throw ResourceNotFound("Schema does not exists.", s"A schema with id $schemaId does not exists in the schema repository at ${FileUtils.getPath(schemaRepositoryFolderPath).toAbsolutePath.toString}")
    }

    // delete schema file from repository
    getFileForSchema(projectId, schemaId).flatMap(file => {
      file.delete()
      val schema: SchemaDefinition = schemaDefinitions(projectId)(schemaId)
      // delete the schema from the in-memory map
      schemaDefinitions(projectId).remove(schemaId)
      // remove the url of the schema
      baseFhirConfig.profileRestrictions -= schema.url
      // Update project
      projectRepository.deleteSchema(projectId, Some(schemaId))
    })
  }

  /**
   * Deletes all schemas associated with a specific project.
   *
   * @param projectId The unique identifier of the project for which schemas should be deleted.
   */
  override def deleteAllSchemas(projectId: String): Future[Unit] = {
    Future {
      // delete schema definitions for the project
      org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(schemaRepositoryFolderPath, projectId).toFile)
      // remove profile restrictions of project schemas
      val schemaUrls: Set[String] = schemaDefinitions.getOrElse(projectId, mutable.Map.empty)
        .values.map(definition => definition.url)
        .toSet
      baseFhirConfig.profileRestrictions --= schemaUrls
      // remove project from the cache
      schemaDefinitions.remove(projectId)
    } flatMap { _ =>
      // delete project schemas
      projectRepository.deleteSchema(projectId)
    }
  }

  /**
   * Gets the file for the given schema ID.
   * This function locates the schema file under schemaRepositoryFolder/projectId/ folder assuming the name of the file is schemaId.json
   *
   * @param schemaDefinition
   * @return
   */
  private def getFileForSchema(projectId: String, schemaId: String): Future[File] = {
    projectRepository.getProject(projectId) map { project =>
      if (project.isEmpty) throw new IllegalStateException(s"This should not be possible. ProjectId: $projectId does not exist in the project folder repository.")
      FileOperations.getFileForEntityWithinProject(schemaRepositoryFolderPath, project.get.id, schemaId)
    }
  }

  /**
   * Parses the given schema folder and initialize a SchemaDefinition map
   *
   * @param schemaRepositoryFolderPath
   * @return
   */
  private def initMap(schemaRepositoryFolderPath: String): Unit = {
    val schemaFolder = FileUtils.getPath(schemaRepositoryFolderPath).toFile
    logger.info(s"Initializing the Schema Repository from path ${schemaFolder.getAbsolutePath}.")
    if (!schemaFolder.exists()) {
      schemaFolder.mkdirs()
    }
    // We may need to give a warning if there are non-directories inside the scheme repository folder.
    schemaFolder.listFiles().foreach(projectFolder => {
      var schemaFiles = Seq.empty[File]
      try {
        // We may need to give a warning if there are non-json files or other directories inside the project folders.
        schemaFiles = IOUtil.getFilesFromFolder(projectFolder, recursively = true, ignoreHidden = true, withExtension = Some(FileExtensions.JSON.toString))
      } catch {
        case e: Throwable => throw FhirMappingException(s"Given folder for the schema repository is not valid at path ${projectFolder.getAbsolutePath}", e)
      }

      // Read each file containing ProfileRestrictions and convert them to SchemaDefinitions
      val projectSchemas: mutable.Map[String, SchemaDefinition] = mutable.Map.empty
      schemaFiles.foreach { schemaFile =>
        val source = Source.fromFile(schemaFile, StandardCharsets.UTF_8.name()) // read the JSON file
        val fileContent = try source.mkString finally source.close()
        try {
          val profileRestrictions: ProfileRestrictions = fhirFoundationResourceParser.parseStructureDefinition(fileContent.parseJson)

          // We send the filename (stripped from its .json extension) as an id to the SchemaDefinition constructor because at this point we know that the file name is unique within the projectFolder
          // It is not possible to have two files with the same name within a folder.
          val schema: SchemaDefinition = simpleStructureDefinitionService.convertToSchemaDefinition(profileRestrictions.id.getOrElse(IOUtil.removeFileExtension(schemaFile.getName)), profileRestrictions)
          // validate the 'type' field of schema definition
          validateSchemaDefinitionType(schema)
          if (FileOperations.checkFileNameMatchesEntityId(schema.id, schemaFile, "schema")) {
            projectSchemas.put(schema.id, schema)
          } // else case is logged within FileOperations.checkFileNameMatchesEntityId
        } catch {
          case e: Throwable =>
            logger.error(s"Failed to parse schema definition at ${schemaFile.getPath}", e)
            System.exit(1)
        }
      }
      if (projectSchemas.isEmpty) {
        // No processable schema files under projectFolder
        logger.warn(s"There are no processable schema files under ${projectFolder.getAbsolutePath}. Skipping ${projectFolder.getName}.")
      } else {
        this.schemaDefinitions.put(projectFolder.getName, projectSchemas)
      }
    })
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
   *
   * @return
   */
  private def initBaseFhirConfig(): BaseFhirConfig = {
    // BaseFhirConfig will act as a validator for the schema definitions by holding the ProfileDefinitions in memory
    val fhirConfigReader: IFhirConfigReader = new FSConfigReader(
      fhirVersion = FhirVersionUtil.getMajorFhirVersion(ToFhirConfig.engineConfig.schemaRepositoryFhirVersion),
      profilesPath = Some(FileUtils.getPath(schemaRepositoryFolderPath).toString))

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
   * @param schemaId  Identifier of the schema definition
   * @return Structure definition of the schema converted into StructureDefinition Resource
   */
  override def getSchemaAsStructureDefinition(projectId: String, schemaId: String): Future[Option[Resource]] = {
    getSchema(projectId, schemaId).map {
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
   * @param projectId                    The identifier of the project in which the schemas will be created.
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
      val schemaId = structureDefinition.id.getOrElse(HashUtil.md5Hash(structureDefinition.url))
      val version = structureDefinition.version.getOrElse(SchemaDefinition.VERSION_LATEST) // We always use the "latest" version if there is no version!
      checkIfSchemaIsUnique(projectId, schemaId, s"${structureDefinition.url}|$version", None)

      // To use convertToSchemaDefinition, profileRestrictions sequence must include the structure definition. Add it before conversion
      baseFhirConfig.profileRestrictions =
        SchemaManagementUtil.updateProfileRestrictionsMap(baseFhirConfig.profileRestrictions, structureDefinition.url, version, structureDefinition)
      // This part is a little ugly because we update baseFhirConfig.profileRestrictions with the version that we evaluate above and then
      //   let concertToSchemaDefinition put a version to the created schemaDefinition. We rely on that both our above version assignment
      //   and convertToSchema uses the same method to evaluate the value for the version.
      val schemaDefinition = simpleStructureDefinitionService.convertToSchemaDefinition(schemaId, structureDefinition)
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
   * Checks if ID of the schema is unique in the project and schema URL (including the version) is unique among all schemas (within all projects)
   * Throws:
   * {@link AlreadyExists} with the code 409 if the schema ID is not unique in the project or the schema URL is not unique
   *
   * @param projectId          Identifier of the project to check schema ID's in it
   * @param schemaId           Identifier of the schema
   * @param schemaCanonicalUrl Canonical Url of the schema with its version
   * @param schemaName         Name of the schema. If provided its uniqueness is checked within the given projectId.
   */
  private def checkIfSchemaIsUnique(projectId: String, schemaId: String, schemaUrl: String, schemaName: Option[String]): Unit = {
    // Check the uniqueness of the schemaId
    schemaDefinitions.get(projectId).flatMap(_.get(schemaId)).foreach { _ =>
      throw AlreadyExists("Schema already exists.", s"A schema definition with id $schemaId already exists in the schema repository at ${FileUtils.getPath(schemaRepositoryFolderPath).toAbsolutePath.toString}")
    }

    // Collect the Canonical URLs of all schema definitions and check the uniqueness of the schemaUrl
    val schemaCanonicalUrls: Map[String, String] = schemaDefinitions.values.flatMap(_.values).map(schema => s"${schema.url}|${schema.version}" -> schema.name).toMap
    schemaCanonicalUrls.get(schemaUrl).foreach { schemaName =>
      throw AlreadyExists("Schema already exists.", s"A schema definition with url $schemaUrl already exists. Check the schema '$schemaName'")
    }

    // If a schemaName exists, check the uniqueness of the schemaName
    schemaName.foreach { name =>
      schemaDefinitions.get(projectId).foreach { schemas =>
        if (schemas.values.exists(_.name == name)) {
          throw AlreadyExists("Schema already exists.", s"A schema definition with name $name already exists in the schema repository at ${FileUtils.getPath(schemaRepositoryFolderPath).toAbsolutePath.toString}")
        }
      }
    }

  }

  /**
   * Write the schema file and update the caches accordingly
   *
   * @param projectId                   Id of the project that will include the schema
   * @param structureDefinitionResource Schema resource that will be written
   * @param schemaDefinition            Definition of the schema
   * @return
   */
  private def writeSchemaAndUpdateCaches(projectId: String, structureDefinitionResource: Resource, schemaDefinition: SchemaDefinition): Future[SchemaDefinition] = {
    getFileForSchema(projectId, schemaDefinition.id).flatMap(newFile => {
      val fw = new FileWriter(newFile)
      fw.write(structureDefinitionResource.toPrettyJson)
      fw.close()

      // Update the caches with the new schema
      baseFhirConfig.profileRestrictions =
        SchemaManagementUtil.updateProfileRestrictionsMap(baseFhirConfig.profileRestrictions, schemaDefinition.url, schemaDefinition.version, fhirFoundationResourceParser.parseStructureDefinition(structureDefinitionResource, includeElementMetadata = true))
      schemaDefinitions.getOrElseUpdate(projectId, mutable.Map.empty).put(schemaDefinition.id, schemaDefinition)

      // Update the project with the schema
      projectRepository.addSchema(projectId, schemaDefinition) map { _ =>
        schemaDefinition
      }
    })
  }

  /**
   * Check SchemaDefinition type starts with uppercase.
   *
   * @param schemaDefinition Definition of the schema
   * @return
   */
  private def validateSchemaDefinitionType(schemaDefinition: SchemaDefinition): Unit = {
    val schemaDefinitionType: String = schemaDefinition.`type`
    if (schemaDefinitionType.isEmpty || schemaDefinitionType.apply(0).isLower) {
      throw BadRequest("Schema definition is not valid.", s"Schema definition type must start with an uppercase letter!")
    }
  }

  /**
   * Validates that all referenced profiles in a given schema either exist as base FHIR definitions,
   * are already present in the system, or are included in the list of schemas to be created.
   *
   * @param projectId      The ID of the project in which the schemas are being validated.
   * @param schemaResource The FHIR StructureDefinition resource representing the schema to be validated.
   * @param schemaUrls     The list of URLs of the schemas that are included in the current batch to be created.
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

  /**
   * Reload the schema definitions from the given folder
   *
   * @return
   */
  override def invalidate(): Unit = {
    this.schemaDefinitions.clear()
    this.baseFhirConfig = initBaseFhirConfig()
    this.simpleStructureDefinitionService = new SimpleStructureDefinitionService(this.baseFhirConfig)
    initMap(schemaRepositoryFolderPath)
  }

  /**
   * Retrieve the projects and SchemaDefinitions within.
   *
   * @return Map of projectId -> Seq[SchemaDefinition]
   */
  override def getProjectPairs: Map[String, Seq[SchemaDefinition]] = {
    schemaDefinitions.map { case (projectId, schemaPairs) =>
      projectId -> schemaPairs.values.toSeq
    }.toMap
  }
}

