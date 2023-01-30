package io.tofhir.server.service

import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.model.FhirMapping
import io.tofhir.server.model.MappingFile
import io.tofhir.server.service.mapping.{IMappingRepository, MappingRepository}

import scala.concurrent.Future

class MappingService(mappingRepositoryFolderPath: String) extends LazyLogging {

  private val mappingRepository: IMappingRepository = new MappingRepository(mappingRepositoryFolderPath)

  /**
   * Get all mapping metadata from the mapping repository
   * @param withSubFolder if given, only return the mappings in the given sub-folder
   * @return Seq[MappingFile]
   */
  def getAllMetadata(withSubFolder: Option[String]): Future[Seq[MappingFile]] = {
    mappingRepository.getAllMappingMetadata(withSubFolder)
  }

  /**
   * Save the mapping to the repository
   * @param directory sub-folder of the mapping
   * @param mapping mapping to save
   * @return FhirMapping
   */
  def createMapping(directory: String, mapping: FhirMapping): Future[FhirMapping] = {
    mappingRepository.createMapping(directory, mapping)
  }

  /**
   * Get the mapping definition by its name and sub-folder
   * @param directory sub-folder of the mapping
   * @param name name of the mapping
   * @return FhirMapping if available
   */
  def getMappingByName(directory: String, name: String): Future[Option[FhirMapping]] = {
    mappingRepository.getMappingByName(directory, name)
  }

  /**
   * Update the mapping in the repository
   * @param directory sub-folder of the mapping
   * @param name name of the mapping
   * @param mapping mapping to update
   * @return
   */
  def updateMapping(directory: String, name: String, mapping: FhirMapping): Future[FhirMapping] = {
    mappingRepository.updateMapping(directory, name, mapping)
  }

  /**
   * Delete the mapping from the repository
   * @param directory sub-folder of the mapping
   * @param name name of the mapping
   * @return
   */
  def removeMapping(directory: String, name: String): Future[Unit] = {
    mappingRepository.removeMapping(directory, name)
  }

}
