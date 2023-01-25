package io.tofhir.server.service.mapping

import io.onfhir.config.IFhirVersionConfigurator
import io.onfhir.r4.config.FhirR4Configurator
import io.tofhir.engine.model.FhirMapping
import io.tofhir.server.model.MappingFile

import scala.concurrent.Future

/**
 * Interface to save and load MappingFiles
 * so that the client applications can manage the mappings through CRUD operations
 */
trait IMappingRepository {

  protected val fhirConfigurator: IFhirVersionConfigurator = new FhirR4Configurator()

  /**
   * Retrieve the metadata of all MappingFile, filter by subfolder if given
   * @return
   */
  def getAllMappingMetadata(withSubFolder: Option[String]): Future[Seq[MappingFile]]

  /**
   * Retrieve the mapping identified by its directory and name.
   * @param directory
   * @param name
   * @return
   */
   def getMappingByName(directory: String, name: String): Future[Option[FhirMapping]]

}
