package io.tofhir.server.service.terminology

import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.model.{FhirMappingJob, LocalFhirTerminologyServiceSettings}
import io.tofhir.server.model.TerminologySystem
import io.tofhir.server.repository.job.IJobRepository
import io.tofhir.server.repository.terminology.ITerminologySystemRepository

import io.tofhir.engine.Execution.actorSystem.dispatcher
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
    // Update TerminologySystem first, then update Jobs Terminology service setting fields
    terminologySystemRepository.updateTerminologySystem(id, terminologySystem).flatMap { terminologySystem =>
      updateJobTerminologyServiceSettings(id, Some(terminologySystem)).map(_ => terminologySystem)
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
      updateJobTerminologyServiceSettings(id).map(_ => terminologySystem)
    }
  }

  /**
   * Find the mappings jobs which refer to the given terminologySystemId and update their terminology service settings
   * with the provided terminologySystem. If the provided terminologySystem is None, remove Job's terminology service settings.
   *
   * @param terminologySystemId id of TerminologySystem
   * @param terminologySystem   TerminologySystem
   * @return
   */
  private def updateJobTerminologyServiceSettings(terminologySystemId: String, terminologySystem: Option[TerminologySystem] = Option.empty): Future[Unit] = {
    Future.sequence(mappingJobRepository.getProjectPairs.flatMap { // Get all mapping jobs grouped under their projects.
      case (projectId, mappingJobs) =>
        mappingJobs.filter { job => // Find the mapping jobs which refer to the given terminologySystemId
          job.terminologyServiceSettings match {
            case Some(settings: LocalFhirTerminologyServiceSettings) =>
              settings.folderPath.split('/').lastOption.contains(terminologySystemId)
            case _ => false
          }
        } map { jobToBeUpdated =>
          // Create updated mapping job object
          val updatedJob = terminologySystem match {
            // Update terminology service case
            case Some(ts) =>
              val terminologyServiceSettings: LocalFhirTerminologyServiceSettings =
                jobToBeUpdated.terminologyServiceSettings.get.asInstanceOf[LocalFhirTerminologyServiceSettings]
              // Update concept maps
              val updatedConceptMaps = terminologyServiceSettings.conceptMapFiles.flatMap(cm =>
                ts.conceptMaps.find(_.id == cm.id)
              )
              // Update code systems
              val updatedCodeSystems = terminologyServiceSettings.codeSystemFiles.flatMap(cs =>
                ts.codeSystems.find(_.id == cs.id)
              )
              val updatedTerminologyServiceSettings: LocalFhirTerminologyServiceSettings = terminologyServiceSettings.copy(conceptMapFiles = updatedConceptMaps, codeSystemFiles = updatedCodeSystems)
              jobToBeUpdated.copy(terminologyServiceSettings = Some(updatedTerminologyServiceSettings))
            // Delete terminology service case
            case None => jobToBeUpdated.copy(terminologyServiceSettings = None)
          }
          // Update job in the repository
          mappingJobRepository.updateJob(projectId, jobToBeUpdated.id, updatedJob)
        }
    }.toSeq).map { _ => () }
  }
}
