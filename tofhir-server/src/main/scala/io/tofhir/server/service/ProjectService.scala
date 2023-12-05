package io.tofhir.server.service

import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.server.model.{BadRequest, Project, ProjectEditableFields}
import io.tofhir.server.service.job.IJobRepository
import io.tofhir.server.service.mapping.IMappingRepository
import io.tofhir.server.service.mappingcontext.IMappingContextRepository
import io.tofhir.server.service.project.IProjectRepository
import io.tofhir.server.service.schema.ISchemaRepository
import org.json4s.JObject
import org.json4s.JsonAST.JString

import scala.concurrent.Future
import scala.util.{Failure, Success}

class ProjectService(projectRepository: IProjectRepository,
                     jobRepository: IJobRepository,
                     mappingRepository: IMappingRepository,
                     mappingContextRepository: IMappingContextRepository,
                     schemaRepository: ISchemaRepository) extends LazyLogging {

  /**
   * Retrieve all Projects
   *
   * @return all projects in the repository
   */
  def getAllProjects: Future[Seq[Project]] = {
    projectRepository.getAllProjects
  }

  /**
   * Save project to the repository.
   *
   * @param project project to be saved
   * @throws BadRequest when the given project is not valid
   * @return the created project
   */
  def createProject(project: Project): Future[Project] = {
    try {
      project.validate()
    } catch {
      case _: IllegalArgumentException => throw BadRequest("Invalid project content !", s"The given id ${project.id} is not a valid UUID.")
    }
    projectRepository.createProject(project)
  }

  /**
   * Retrieve the project identified by its id.
   *
   * @param id id of the project
   * @return the project
   */
  def getProject(id: String): Future[Option[Project]] = {
    projectRepository.getProject(id)
  }

  /**
   * Update the some fields of a project in the repository.
   *
   * @param id      id of the project
   * @param project patch to be applied to the project
   * @return
   */
  def updateProject(id: String, project: JObject): Future[Project] = {
    // validate patch
    validateProjectPatch(project)
    projectRepository.updateProject(id, project)
  }

  /**
   * Delete project from the repository
   *
   * @param id id of the project
   * @return
   */
  def removeProject(id: String): Future[Unit] = {
    val jobIds: Seq[String] = projectRepository.getJobIds(id)
    val mappingIds: Seq[String] = projectRepository.getMappingIds(id)
    val mappingContextIds: Seq[String] = projectRepository.getMappingContextIds(id)
    val schemaIds: Seq[String] = projectRepository.getSchemaIds(id)

    Future {
      // Delete the folders
      projectRepository.removeProject(id) onComplete {
        case Success(v) =>
          // Delete elements in the project, if any of the deletion is failed log the error and continue
          jobIds.foreach(jobId => {
            try{
              jobRepository.deleteJob(id, jobId)
            }catch {
              case e: Throwable => logger.error(s"Error while deleting the job: ${jobId}", e)
            }
          })
          mappingIds.foreach(mappingId => {
            try {
              mappingRepository.deleteMapping(id, mappingId)
            } catch {
              case e: Throwable => logger.error(s"Error while deleting the mapping: ${mappingId}", e)
            }
          })
          mappingContextIds.foreach(mappingContextId => {
            try {
              mappingContextRepository.deleteMappingContext(id, mappingContextId)
            } catch {
              case e: Throwable => logger.error(s"Error while deleting the mapping context: ${mappingContextId}", e)
            }
          })
          schemaIds.foreach(schemaId => {
            try {
              schemaRepository.deleteSchema(id, schemaId)
            } catch {
              case e: Throwable => logger.error(s"Error while deleting the schema: ${schemaId}", e)
            }
          })
        // If project deletion is failed return the exception thrown in removeProject
        case Failure(exception) => throw exception
      }
    }
  }

  /**
   * Validates the given project patch. It ensures that the patch includes updates only for the editable fields of a project.
   *
   * @param patch the patch
   * @throws BadRequest when the given patch is invalid
   */
  private def validateProjectPatch(patch: JObject): Unit = {
    if (patch.obj.isEmpty || !patch.obj.forall {
      case (ProjectEditableFields.DESCRIPTION, JString(_)) => true
      case _ => false
    })
      throw BadRequest("Invalid Patch!", "Invalid project patch!")
  }
}
