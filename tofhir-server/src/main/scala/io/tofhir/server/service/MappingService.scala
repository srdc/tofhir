package io.tofhir.server.service

import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.model.FhirMapping
import io.tofhir.server.common.model.{BadRequest, ResourceNotFound}
import io.tofhir.server.repository.job.IJobRepository
import io.tofhir.server.repository.mapping.IMappingRepository

import io.tofhir.engine.Execution.actorSystem.dispatcher
import scala.concurrent.Future

class MappingService(mappingRepository: IMappingRepository, jobRepository: IJobRepository) extends LazyLogging {

  /**
   * Get all mapping metadata from the mapping repository
   *
   * @param projectId if given, only return the mappings in the given sub-folder
   * @return Seq[MappingFile]
   */
  def getAllMappings(projectId: String): Future[Seq[FhirMapping]] = {
    mappingRepository.getAllMappings(projectId)
  }

  /**
   * Save the mapping to the repository
   *
   * @param projectId sub-folder of the mapping
   * @param mapping   mapping to save
   * @return FhirMapping
   */
  def createMapping(projectId: String, mapping: FhirMapping): Future[FhirMapping] = {
    mappingRepository.saveMapping(projectId, mapping)
  }

  /**
   * Get the mapping by its id
   *
   * @param projectId project id the mapping belongs to
   * @param mappingId mapping id
   * @return
   */
  def getMapping(projectId: String, mappingId: String): Future[Option[FhirMapping]] = {
    mappingRepository.getMapping(projectId, mappingId)
  }

  /**
   * Update the mapping from the repository
   * If the mapping url is changed, update the mapping url in all jobs that reference this mapping
   *
   * @param projectId Project id the mapping belongs to
   * @param mappingId Unique identifier of the FhirMapping
   * @param mapping   The updated FhirMapping
   * @return
   */
  def updateMapping(projectId: String, mappingId: String, mapping: FhirMapping): Future[FhirMapping] = {
    // Ensure that the provided schemaId matches the SchemaDefinition's schemaId
    if (!mappingId.equals(mapping.id)) {
      throw BadRequest("Mapping definition is not valid.", s"Identifier of the mapping definition: ${mapping.id} does not match with the provided mappingId: $mappingId in the path!")
    }
    mappingRepository.updateMapping(projectId, mappingId, mapping)
  }

  /**
   * Delete the mapping from the repository if it is not referenced by any job
   *
   * @param projectId project id the mapping belongs to
   * @param mappingId mapping id
   * @return
   */
  def deleteMapping(projectId: String, mappingId: String): Future[Unit] = {
    // find mapping url from mapping id
    mappingRepository.getMapping(projectId, mappingId).flatMap { mapping =>
      //find all jobs that reference this mapping
      val mappingUrl = mapping.getOrElse(
        throw ResourceNotFound("Mapping does not exists.", s"A mapping with id $mappingId does not exists in the mapping repository.")
      ).url
      jobRepository.getJobsReferencingMapping(projectId, mappingUrl).flatMap { jobs =>
        if (jobs.isEmpty)
          mappingRepository.deleteMapping(projectId, mappingId)
        else
          throw BadRequest("Mapping is referenced by jobs.", s"The mapping with URL $mappingUrl is referenced by jobs: ${jobs.map(_.id).mkString(", ")}.")
      }
    }
  }

}
