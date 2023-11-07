package io.tofhir.server.service

import com.typesafe.scalalogging.LazyLogging
import io.tofhir.server.model.{BadRequest, Project, ProjectEditableFields}
import io.tofhir.server.service.job.IJobRepository
import io.tofhir.server.service.mapping.IMappingRepository
import io.tofhir.server.service.mappingcontext.IMappingContextRepository
import io.tofhir.server.service.project.IProjectRepository
import io.tofhir.server.service.schema.ISchemaRepository
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
   * Delete project from the repository
   *
   * @param id id of the project
   * @return
   */
  def removeProject(id: String): Future[Unit] = {
    val jobIds: Seq[String] = projectRepository.getJobIds(id)
    jobIds.foreach( jobId => {
      jobRepository.deleteJob(id, jobId)
    })
    val mappingIds: Seq[String] = projectRepository.getMappingIds(id)
    mappingIds.foreach(mappingId => {
      mappingRepository.deleteMapping(id, mappingId)
    })
    val mappingContextIds: Seq[String] = projectRepository.getMappingContextIds(id)
    mappingContextIds.foreach(mappingContextId => {
      mappingContextRepository.deleteMappingContext(id, mappingContextId)
    })
    val schemaIds: Seq[String] = projectRepository.getSchemaIds(id)
    schemaIds.foreach(schemaId => {
      schemaRepository.deleteSchema(id, schemaId)
    })

    projectRepository.removeProject(id)
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
