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
   * Save the mapping to the repository.
   *
   * @param directory  subfolder to save the mapping in
   * @param mapping mapping to save
   * @return
   */
  def createMapping(directory: String, mapping: FhirMapping): Future[FhirMapping]

  /**
   * Retrieve the mapping identified by its directory and name.
   * @param directory subfolder the mapping is in
   * @param name name of the mapping
   * @return
   */
  def getMappingByName(directory: String, name: String): Future[Option[FhirMapping]]

  /**
   * Update the mapping in the repository.
   * @param directory subfolder the mapping is in
   * @param name name of the mapping
   * @param mapping mapping to update
   * @return
   */
  def updateMapping(directory: String, name: String, mapping: FhirMapping): Future[FhirMapping]

  /**
   * Delete the mapping from the repository.
   * @param directory subfolder the mapping is in
   * @param name name of the mapping
   * @return
   */
  def removeMapping(directory: String, name: String): Future[Unit]

}
