package io.tofhir.server.service.terminology

import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.model.{FhirMappingJob, LocalFhirTerminologyServiceSettings}
import io.tofhir.server.model.TerminologySystem
import io.tofhir.server.repository.job.IJobRepository
import io.tofhir.server.repository.terminology.ITerminologySystemRepository

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class TerminologySystemService(terminologySystemRepository: ITerminologySystemRepository, mappingJobRepository: IJobRepository) extends LazyLogging {

  /**
   * Get all TerminologySystem metadata from the TerminologySystem repository
   *
   * @return List of TerminologySystem metadata
   */
  def getAllMetadata: Future[Seq[TerminologySystem]] = {
    terminologySystemRepository.getTerminologySystemsMetadata
  }

  /**
   * Create a new TerminologySystem
   *
   * @param terminologySystem The TerminologySystem will be created
   * @return Created TerminologySystem
   */
  def createTerminologySystem(terminologySystem: TerminologySystem): Future[TerminologySystem] = {
    terminologySystemRepository.createTerminologySystem(terminologySystem)
  }

  /**
   * Get the TerminologySystem by id
   *
   * @param id id of the TerminologySystem
   * @return The TerminologySystem with given id
   */
  def getTerminologySystem(id: String): Future[Option[TerminologySystem]] = {
    terminologySystemRepository.getTerminologySystem(id)
  }

  /**
   * Update the TerminologySystem
   *
   * @param id                TerminologySystem id
   * @param terminologySystem TerminologySystem to update
   * @return Updated TerminologySystem
   */
  def updateTerminologySystem(id: String, terminologySystem: TerminologySystem): Future[TerminologySystem] = {
    // Update TerminologySystem first, then update Jobs Terminology fields
    terminologySystemRepository.updateTerminologySystem(id, terminologySystem).flatMap { terminologySystem =>
      // update Jobs Terminology fields if it is successful
      updateJobTerminology(id, Some(terminologySystem)).map(_ => terminologySystem)
    }
  }

  /**
   * Delete the TerminologySystem
   *
   * @param id TerminologySystem id
   * @return
   */
  def deleteTerminologySystem(id: String): Future[Unit] = {
    // Delete terminology system first, then update Jobs Terminology fields
    terminologySystemRepository.deleteTerminologySystem(id).flatMap { terminologySystem =>
      // update Jobs Terminology fields if it is successful
      updateJobTerminology(id).map(_ => terminologySystem)
    }
  }

  /**
   * Check whether job's id and terminologySystem id is equal
   * @param job job object to be checked
   * @param terminologySystemId id of terminogySystem
   * @return Boolean indicating whether job's id and terminologySystem's id are equal
   */
  private def checkEqualityOfIds(job: FhirMappingJob, terminologySystemId: String): Boolean = {
    // Get terminology service of the job
    val terminologyServiceSettings: LocalFhirTerminologyServiceSettings =
      job.terminologyServiceSettings.get.asInstanceOf[LocalFhirTerminologyServiceSettings]

    terminologyServiceSettings.folderPath.split('/').lastOption.get.equals(terminologySystemId)
  }

  /**
   * Update Job's terminology fields with the updated terminology system.
   *
   * @param id                id of terminologySystem
   * @param terminologySystem updated terminology system
   * @return
   */
  private def updateJobTerminology(id: String, terminologySystem: Option[TerminologySystem] = Option.empty): Future[Unit] = {
    // Get project ids from job cache
    val projectIds: Seq[String] = mappingJobRepository.getCachedMappingsJobs.keys.toSeq

    Future.sequence(projectIds.map(projectId => {
      // Get jobs of given project
      mappingJobRepository.getAllJobs(projectId).flatMap { jobs =>
        Future.sequence(jobs.filter(job => {
          job.terminologyServiceSettings.isDefined && job.terminologyServiceSettings.get.isInstanceOf[LocalFhirTerminologyServiceSettings] && checkEqualityOfIds(job, id)
        }).map(jobToBeUpdated => {
          // Create updated job object
          val updatedJob = terminologySystem match {
            // Update terminology service case
            case Some(ts) => {
              val terminologyServiceSettings: LocalFhirTerminologyServiceSettings =
                jobToBeUpdated.terminologyServiceSettings.get.asInstanceOf[LocalFhirTerminologyServiceSettings]

              val updatedTerminologyServiceSettings: LocalFhirTerminologyServiceSettings =
                terminologyServiceSettings.copy(conceptMapFiles = ts.conceptMaps, codeSystemFiles = ts.codeSystems)

              jobToBeUpdated.copy(terminologyServiceSettings = Some(updatedTerminologyServiceSettings))
            }
            // Delete terminology service case
            case None => jobToBeUpdated.copy(terminologyServiceSettings = None)
          }
          // Update job in the repository
          mappingJobRepository.putJob(projectId, jobToBeUpdated.id, updatedJob)
        }))
      }
    })).map {_ => ()}
  }
}
