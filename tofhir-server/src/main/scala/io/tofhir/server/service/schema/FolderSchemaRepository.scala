package io.tofhir.server.service.schema

import io.onfhir.api
import io.onfhir.api.util.IOUtil
import io.onfhir.config.{BaseFhirConfig, FSConfigReader, IFhirConfigReader}
import io.onfhir.util.JsonFormatter._
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.model.FhirMappingException
import io.tofhir.engine.util.FileUtils
import io.tofhir.engine.util.FileUtils.FileExtensions
import io.tofhir.server.model.{AlreadyExists, BadRequest, InternalError, ResourceNotFound, SchemaDefinition}
import io.tofhir.server.service.SimpleStructureDefinitionService

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
class FolderSchemaRepository(schemaRepositoryFolderPath: String) extends AbstractSchemaRepository {

  private val fhirConfigReader: IFhirConfigReader = new FSConfigReader(profilesPath = Some(schemaRepositoryFolderPath))

  // BaseFhirConfig will act as a cache by holding the ProfileDefinitions in memory
  private var baseFhirConfig: BaseFhirConfig = fhirConfigurator.initializePlatform(fhirConfigReader)
  private var simpleStructureDefinitionService = new SimpleStructureDefinitionService(baseFhirConfig)

  private var nameUrlMap: mutable.Map[String, String] = NameUrlPair.initMap(schemaRepositoryFolderPath)
  /**
   * Reload from the folder in case schema definitions are updated directly through the file system or through other means.
   */
  private def reload(): Unit = {
    nameUrlMap = NameUrlPair.initMap(schemaRepositoryFolderPath)
    baseFhirConfig = fhirConfigurator.initializePlatform(fhirConfigReader)
    simpleStructureDefinitionService = new SimpleStructureDefinitionService(baseFhirConfig)
  }

  /**
   * Retrieve the metadata of all SchemaDefinitions (only url, type and name fields are populated)
   *
   * @param withReload If true, reload all definitions from the repository implementation (from disk)
   * @return
   */
  override def getAllSchemaMetadata(withReload: Boolean): Future[Seq[SchemaDefinition]] = {
    Future {
      if (withReload) reload()
      baseFhirConfig.profileRestrictions
        .filterNot(_._1.startsWith(api.FHIR_ROOT_URL_FOR_DEFINITIONS))
        .toSeq
        .map { case (url, pr) =>
          SchemaDefinition(url, pr.resourceType, pr.resourceName.getOrElse(pr.resourceType), None, None)
        }
    }
  }

  /**
   * Retrieve the schema identified by its URL.
   *
   * @param url
   * @param withReload If true, reload all definitions from the repository implementation (from disk)
   * @return
   */
  override def getSchemaByUrl(url: String, withReload: Boolean): Future[Option[SchemaDefinition]] = {
    Future {
      if (withReload) reload()
      baseFhirConfig.profileRestrictions
        .find(p => p._1 == url)
        .map(t => convertToSchemaDefinition(t._2, simpleStructureDefinitionService))
    }
  }

  /**
   * Retrieve the schema identified by its name.
   *
   * @param name
   * @param withReload If true, reload all definitions from the repository implementation (from disk)
   * @return
   */
  override def getSchemaByName(name: String, withReload: Boolean): Future[Option[SchemaDefinition]] = {
    Future {
      if (withReload) reload()
      val filteredMap = baseFhirConfig.profileRestrictions
        .filter(p => p._2.resourceName.getOrElse(p._2.resourceType) == name)
      if (filteredMap.size > 1) throw new IllegalStateException(s"There are ${filteredMap.size} schema definitions with the same name/rootPath!")
      filteredMap
        .find(p => p._2.resourceName.getOrElse(p._2.resourceType) == name)
        .map(t => convertToSchemaDefinition(t._2, simpleStructureDefinitionService))
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

      // Write to the repository as a new file
      val newFile = FileUtils.getPath(schemaRepositoryFolderPath, s"${schemaDefinition.name}${FileExtensions.StructureDefinition}${FileExtensions.JSON}").toFile
      if (newFile.exists()) {
        throw AlreadyExists("Schema already exists.", s"A schema definition with name ${schemaDefinition.name} already exists in the schema repository at ${FileUtils.getPath(schemaRepositoryFolderPath).toAbsolutePath.toString}")
      }
      val fw = new FileWriter(newFile)
      import io.onfhir.util.JsonFormatter._
      fw.write(structureDefinitionResource.toPrettyJson)
      fw.close()

      // Update the baseConfig since we use it as a cache
      nameUrlMap += schemaDefinition.name -> schemaDefinition.url
      baseFhirConfig.profileRestrictions += schemaDefinition.url -> fhirFoundationResourceParser.parseStructureDefinition(structureDefinitionResource, includeElementMetadata = true)
    } flatMap { _ =>
      getSchemaByUrl(schemaDefinition.url, withReload = false) map {
        case Some(sch) => sch
        case None => throw InternalError("Error while creating the new schema.", s"Cannot find the newly created schema definition: ${schemaDefinition.url}")
      }
    }
  }

  /**
   * Update the schema in the repository.
   * @param name Type of the schema
   * @param schemaDefinition Schema definition
   * @return
   */
  override def putSchema(name: String, schemaDefinition: SchemaDefinition): Future[Unit] = {
    Future {
      // Validate
      val structureDefinitionResource = convertToStructureDefinitionResource(schemaDefinition)
      try {
        fhirConfigurator.validateGivenInfrastructureResources(baseFhirConfig, api.FHIR_FOUNDATION_RESOURCES.FHIR_STRUCTURE_DEFINITION, Seq(structureDefinitionResource))
      } catch {
        case e: Exception =>
          throw BadRequest("Schema definition is not valid.", s"Schema definition cannot be validated: ${schemaDefinition.url}", Some(e))
      }
      // Find
      val fileName = name + FileExtensions.StructureDefinition.toString + FileExtensions.JSON.toString
      val file = FileUtils.findFileByName(schemaRepositoryFolderPath, fileName)
      if (file.isEmpty) {
        throw ResourceNotFound("Schema does not exist.", s"A schema definition with name $name does not exist in the schema repository at ${FileUtils.getPath(schemaRepositoryFolderPath).toAbsolutePath.toString}")
      }
      // Update
      val fw = new FileWriter(file.get)
      import io.onfhir.util.JsonFormatter._
      fw.write(structureDefinitionResource.toPrettyJson)
      fw.close()

      // Update cache
      baseFhirConfig.profileRestrictions += schemaDefinition.url -> fhirFoundationResourceParser.parseStructureDefinition(structureDefinitionResource, includeElementMetadata = true)
    }
  }

  /**
   * Delete the schema from the repository.
   * @param name Name of the schema
   * @return
   */
  override def deleteSchema(name: String): Future[Unit] = {
    Future {
      val fileName = name + FileExtensions.StructureDefinition.toString + FileExtensions.JSON.toString
      val file = FileUtils.findFileByName(schemaRepositoryFolderPath, fileName)
      if (file.isEmpty) {
        throw ResourceNotFound("Schema does not exist.", s"A schema definition with name $name does not exist in the schema repository at ${FileUtils.getPath(schemaRepositoryFolderPath).toAbsolutePath.toString}")
      }
      file.get.delete()
      // Update cache
      nameUrlMap.get(name) match {
        case Some(url) =>
          baseFhirConfig.profileRestrictions -= url
          nameUrlMap -= name
        case None =>
      }
    }
  }
}

object NameUrlPair {
  def initMap(schemaRepositoryFolderPath: String): mutable.Map[String, String] = {
    val nameUrlMap = mutable.Map[String, String]()
    val folder = new File(schemaRepositoryFolderPath)
    var files = Seq.empty[File]
    try {
      files = IOUtil.getFilesFromFolder(folder, withExtension = Some(FileExtensions.JSON.toString), recursively = Some(true))
    } catch {
      case e: Throwable => throw FhirMappingException(s"Given folder for the mapping repository is not valid.", e)
    }
    files.map { f =>
      val source = Source.fromFile(f, StandardCharsets.UTF_8.name()) // read the JSON file
      val fileContent = try source.mkString finally source.close()
      val schema = fileContent.parseJson.extractOpt[SchemaDefinition]
      schema match {
        case Some(schema) =>
          nameUrlMap += schema.name -> schema.url
        case None =>
          throw FhirMappingException(s"Given mapping file is not valid: ${f.getAbsolutePath}")
      }
    }
    nameUrlMap
  }
}
