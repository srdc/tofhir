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
   * Get the mapping definition by its name and sub-folder
   * @param directory sub-folder of the mapping
   * @param name name of the mapping
   * @return FhirMapping if available
   */
  def getMappingByName(directory: String, name: String): Future[Option[FhirMapping]] = {
    mappingRepository.getMappingByName(directory, name)
  }

}
