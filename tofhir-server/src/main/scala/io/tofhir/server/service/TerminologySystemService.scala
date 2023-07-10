package io.tofhir.server.service

import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.model.{FhirMappingJob, LocalFhirTerminologyServiceSettings}
import io.tofhir.server.model.TerminologySystem
import io.tofhir.server.service.job.IJobRepository
import io.tofhir.server.service.project.IProjectRepository
import io.tofhir.server.service.terminology.ITerminologySystemRepository

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class TerminologySystemService(terminologySystemRepository: ITerminologySystemRepository, mappingJobRepository: IJobRepository, projectRepository: IProjectRepository) extends LazyLogging {

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

    // Get project ids from project repository
    val projectIds: Future[Seq[String]] = projectRepository.getAllProjects.map(_.map(_.id))

    projectIds.flatMap { projectIds =>
        Future.traverse(projectIds) { projectId =>

        // Get jobs of given project
        val jobs: Future[Seq[FhirMappingJob]] = mappingJobRepository.getAllJobs(projectId)

        jobs.map { jobs =>
          jobs
            .filter { job =>
              job.terminologyServiceSettings.isDefined && job.terminologyServiceSettings.get.isInstanceOf[LocalFhirTerminologyServiceSettings]
            }
            .foreach { job =>
              // Get terminology service of the job
              val terminologyServiceSettings: LocalFhirTerminologyServiceSettings =
                job.terminologyServiceSettings.get.asInstanceOf[LocalFhirTerminologyServiceSettings]

              // Skip if id is not equal
              if (terminologyServiceSettings.folderPath.split('/').lastOption.get.equals(terminologySystem.id)) {

                // Update terminology service object
                val updatedTerminologyServiceSettings: LocalFhirTerminologyServiceSettings =
                  terminologyServiceSettings.copy(conceptMapFiles = terminologySystem.conceptMaps,
                    codeSystemFiles = terminologySystem.codeSystems)

                // Update job object
                val updatedJob = job.copy(terminologyServiceSettings = Some(updatedTerminologyServiceSettings))

                // Update job repository
                mappingJobRepository.putJob(projectId, job.id, updatedJob)
              }
            }
        }
      }
    }

    // Update TerminologySystem
    terminologySystemRepository.updateTerminologySystem(id, terminologySystem)
  }

  /**
   * Delete the TerminologySystem
   * @param id TerminologySystem id
   * @return
   */
  def deleteTerminologySystem(id: String): Future[Unit] = {

    // Get project ids from project repository
    val projectIds: Future[Seq[String]] = projectRepository.getAllProjects.map(_.map(_.id))

    projectIds.flatMap { projectIds =>
      Future.traverse(projectIds) { projectId =>

        // Get jobs of given project
        val jobs: Future[Seq[FhirMappingJob]] = mappingJobRepository.getAllJobs(projectId)

        jobs.map { jobs =>
          jobs
            .filter { job =>
              job.terminologyServiceSettings.isDefined && job.terminologyServiceSettings.get.isInstanceOf[LocalFhirTerminologyServiceSettings]
            }
            .foreach { job =>
              // Get terminology service of the job
              val terminologyServiceSettings: LocalFhirTerminologyServiceSettings =
                job.terminologyServiceSettings.get.asInstanceOf[LocalFhirTerminologyServiceSettings]

              // Skip if id is not equal
              if (terminologyServiceSettings.folderPath.split('/').lastOption.get.equals(id)) {

                // Update job object with deleting terminology service settings
                val updatedJob = job.copy(terminologyServiceSettings = None)

                // Update job repository
                mappingJobRepository.putJob(projectId, job.id, updatedJob)
              }
            }
        }
      }
    }
    // Delete terminology system
    terminologySystemRepository.deleteTerminologySystem(id)
  }
}
