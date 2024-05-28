package io.tofhir.server.service

import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.model.FhirMapping
import io.tofhir.server.common.model.{BadRequest, ResourceNotFound}
import io.tofhir.server.service.job.IJobRepository
import io.tofhir.server.service.mapping.IMappingRepository

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class MappingService(mappingRepository: IMappingRepository, jobRepository: IJobRepository) extends LazyLogging {

  /**
   * Get all mapping metadata from the mapping repository
   * @param projectId if given, only return the mappings in the given sub-folder
   * @return Seq[MappingFile]
   */
  def getAllMappings(projectId: String): Future[Seq[FhirMapping]] = {
    mappingRepository.getAllMappings(projectId)
  }

  /**
   * Save the mapping to the repository
   * @param projectId sub-folder of the mapping
   * @param mapping mapping to save
   * @return FhirMapping
   */
  def createMapping(projectId: String, mapping: FhirMapping): Future[FhirMapping] = {
    mappingRepository.createMapping(projectId, mapping)
  }

  /**
   * Get the mapping by its id
   * @param projectId project id the mapping belongs to
   * @param id mapping id
   * @return
   */
  def getMapping(projectId: String, id: String): Future[Option[FhirMapping]] = {
    mappingRepository.getMapping(projectId, id)
  }

  /**
   * Update the mapping from the repository
   * If the mapping url is changed, update the mapping url in all jobs that reference this mapping
   * @param projectId project id the mapping belongs to
   * @param id mapping id
   * @param mapping mapping to update
   * @return
   */
  def updateMapping(projectId: String, id: String, mapping: FhirMapping): Future[FhirMapping] = {
    // find jobs that reference this mapping and update the mapping url
    mappingRepository.getMapping(projectId, id).flatMap { oldMapping =>
      val oldMappingUrl = oldMapping.getOrElse(
        throw ResourceNotFound("Mapping does not exists.", s"A mapping with id $id does not exists in the mapping repository.")
      ).url
      // update each job that references this mapping
      mappingRepository.putMapping(projectId, id, mapping).flatMap { mapping =>
        jobRepository.getJobsReferencingMapping(projectId, oldMappingUrl).flatMap { jobs =>
          Future.sequence(jobs.map { job =>
            jobRepository.putJob(projectId, job.id, job.copy(
              // find mapping url and change mapping url in mapping tasks in job
              mappings = job.mappings.map(mappingTask =>
                if (mappingTask.mappingRef == oldMappingUrl) mappingTask.copy(mappingRef = mapping.url) else mappingTask)
            ))
          }).map(_ => mapping)
        }
      }

    }
  }

  /**
   * Delete the mapping from the repository if it is not referenced by any job
   * @param projectId project id the mapping belongs to
   * @param id mapping id
   * @return
   */
  def deleteMapping(projectId: String, id: String): Future[Unit] = {
    // find mapping url from mapping id
    mappingRepository.getMapping(projectId, id).flatMap { mapping =>
      //find all jobs that reference this mapping
      val mappingUrl = mapping.getOrElse(
        throw ResourceNotFound("Mapping does not exists.", s"A mapping with id $id does not exists in the mapping repository.")
      ).url
      jobRepository.getJobsReferencingMapping(projectId, mappingUrl).flatMap { jobs =>
        if (jobs.isEmpty)
          mappingRepository.deleteMapping(projectId, id)
        else
          throw BadRequest("Mapping is referenced by jobs.", s"The mapping with URL $mappingUrl is referenced by jobs: ${jobs.map(_.id).mkString(", ")}.")
      }
    }
  }

}
