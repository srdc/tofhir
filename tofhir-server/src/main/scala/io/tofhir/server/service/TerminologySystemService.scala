package io.tofhir.server.service

import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.model.{FhirMappingJob, LocalFhirTerminologyServiceSettings}
import io.tofhir.server.model.TerminologySystem
import io.tofhir.server.service.job.IJobRepository
import io.tofhir.server.service.terminology.ITerminologySystemRepository

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class TerminologySystemService(terminologySystemRepository: ITerminologySystemRepository, mappingJobRepository: IJobRepository) extends LazyLogging {

  /**
   * Get all TerminologySystem metadata from the TerminologySystem repository
   * @return List of TerminologySystem metadata
   */
  def getAllMetadata: Future[Seq[TerminologySystem]] = {
    terminologySystemRepository.getTerminologySystemsMetadata
  }

  /**
   * Create a new TerminologySystem
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
   * @param id TerminologySystem id
   * @param terminologySystem TerminologySystem to update
   * @return Updated TerminologySystem
   */
  def updateTerminologySystem(id: String, terminologySystem: TerminologySystem): Future[TerminologySystem] = {
    // Update TerminologySystem
    val result: Future[TerminologySystem] = terminologySystemRepository.updateTerminologySystem(id, terminologySystem)

    result.map{ terminologySystem =>
      // update Jobs Terminology fields if it is successful
      updateJobTerminology(id ,Some(terminologySystem))
      terminologySystem
    }
  }

  /**
   * Delete the TerminologySystem
   * @param id TerminologySystem id
   * @return
   */
  def deleteTerminologySystem(id: String): Future[Unit] = {

    // Delete terminology system
    val result: Future[Unit] = terminologySystemRepository.deleteTerminologySystem(id)

    result.map{ terminologySystem =>
      // update Jobs Terminology fields if it is successful
      updateJobTerminology(id)
      terminologySystem
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
   * Update Job's terminology fields with updated terminology system
   * @param id id of terminologySystem
   * @param terminologySystem updated terminology system
   * @return
   */
  private def updateJobTerminology(id: String, terminologySystem: Option[TerminologySystem] = Option.empty): Future[Unit] = {
    Future{
        // Get project ids from job cache
        val projectIds: Seq[String] = mappingJobRepository.getCachedMappingsJobs.keys.toSeq

        projectIds.foreach { projectId =>
          // Get jobs of given project
          val jobs: Future[Seq[FhirMappingJob]] = mappingJobRepository.getAllJobs(projectId)

          jobs.map { jobs =>
            jobs
              .filter { job =>
                job.terminologyServiceSettings.isDefined
              }
              .filter { job =>
                job.terminologyServiceSettings.get.isInstanceOf[LocalFhirTerminologyServiceSettings]
              }
              .filter { job =>
                checkEqualityOfIds(job, id) // Skip if id is not equal
              }
              .foreach { job =>
                // Create updated job object
                val updatedJob = terminologySystem match {
                  // Update terminology service case
                  case Some(ts) => {
                    val terminologyServiceSettings: LocalFhirTerminologyServiceSettings =
                      job.terminologyServiceSettings.get.asInstanceOf[LocalFhirTerminologyServiceSettings]

                    val updatedTerminologyServiceSettings: LocalFhirTerminologyServiceSettings =
                      terminologyServiceSettings.copy(conceptMapFiles = ts.conceptMaps, codeSystemFiles = ts.codeSystems)

                    job.copy(terminologyServiceSettings = Some(updatedTerminologyServiceSettings))
                  }
                  // Delete terminology service case
                  case None => job.copy(terminologyServiceSettings = None)
                }
                // Update job in the repository
                mappingJobRepository.putJob(projectId, job.id, updatedJob)
              }
          }
        }
      }
    }
}
