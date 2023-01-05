package io.tofhir.server.service.schema

import io.onfhir.api
import io.onfhir.config.{BaseFhirConfig, FSConfigReader, IFhirConfigReader}
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.util.FileUtils
import io.tofhir.engine.util.FileUtils.FileExtensions
import io.tofhir.server.model.{AlreadyExists, BadRequest, InternalError, SchemaDefinition}
import io.tofhir.server.service.SimpleStructureDefinitionService

import java.io.FileWriter
import scala.concurrent.Future

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

  /**
   * Reload from the folder in case schema definitions are updated directly through the file system or through other means.
   */
  private def reload(): Unit = {
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
          SchemaDefinition(url, pr.resourceType, pr.resourceName, None, None)
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
  override def getSchema(url: String, withReload: Boolean): Future[Option[SchemaDefinition]] = {
    Future {
      if (withReload) reload()
      baseFhirConfig.profileRestrictions
        .find(p => p._1 == url)
        .map(t => convertToSchemaDefinition(t._2, simpleStructureDefinitionService))
    }
  }

  /**
   * Retrieve the schema identified by its type.
   *
   * @param `type`
   * @param withReload If true, reload all definitions from the repository implementation (from disk)
   * @return
   */
  override def getSchemaByType(`type`: String, withReload: Boolean): Future[Option[SchemaDefinition]] = {
    Future {
      if (withReload) reload()
      val filteredMap = baseFhirConfig.profileRestrictions
        .filter(p => p._2.resourceType == `type`)
      if (filteredMap.size > 1) throw new IllegalStateException(s"There are ${filteredMap.size} schema definitions with the same type/rootPath!")
      filteredMap
        .find(p => p._2.resourceType == `type`)
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
      val newFile = FileUtils.getPath(schemaRepositoryFolderPath, s"${schemaDefinition.`type`}.${FileExtensions.CSV}").toFile
      if (newFile.exists()) {
        throw AlreadyExists("Schema already exists.", s"A schema definition with name ${schemaDefinition.`type`} already exists in the schema repository at ${FileUtils.getPath(schemaRepositoryFolderPath).toAbsolutePath.toString}")
      }
      val fw = new FileWriter(newFile)
      import io.onfhir.util.JsonFormatter._
      fw.write(structureDefinitionResource.toPrettyJson)
      fw.close()

      // Update the baseConfig since we use it as a cache
      baseFhirConfig.profileRestrictions += schemaDefinition.url -> fhirFoundationResourceParser.parseStructureDefinition(structureDefinitionResource, includeElementMetadata = true)
    } flatMap { _ =>
      getSchema(schemaDefinition.url, withReload = false) map {
        case Some(sch) => sch
        case None => throw InternalError("Error while creating the new schema.", s"Cannot find the newly created schema definition: ${schemaDefinition.url}")
      }
    }
  }
}
