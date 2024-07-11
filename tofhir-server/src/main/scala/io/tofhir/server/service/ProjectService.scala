package io.tofhir.server.service

import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.server.common.model.BadRequest
import io.tofhir.server.model.{Project, ProjectEditableFields}
import io.tofhir.server.repository.job.IJobRepository
import io.tofhir.server.repository.mapping.IMappingRepository
import io.tofhir.server.repository.mappingContext.IMappingContextRepository
import io.tofhir.server.repository.project.IProjectRepository
import io.tofhir.server.repository.schema.ISchemaRepository
import org.json4s.JObject
import org.json4s.JsonAST.JString

import scala.concurrent.Future

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
   * Removes a project and associated resources.
   *
   * @param id The unique identifier of the project to be removed.
   */
  def removeProject(id: String): Future[Unit] = {
    // first delete the project from repository
    projectRepository.removeProject(id)
      // if project deletion is failed throw the error
      .recover {case e: Throwable => throw e}
      // else delete jobs, mappings, mapping contexts and schemas as well
      .map(_=> {
        jobRepository.deleteProjectJobs(id)
        mappingRepository.deleteProjectMappings(id)
        mappingContextRepository.deleteProjectMappingContexts(id)
        schemaRepository.deleteProjectSchemas(id)
      })
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
