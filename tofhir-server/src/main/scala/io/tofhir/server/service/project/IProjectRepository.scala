package io.tofhir.server.service.project

import io.onfhir.config.IFhirVersionConfigurator
import io.onfhir.r4.config.FhirR4Configurator
import io.tofhir.engine.model.Project
import org.json4s.JObject

import scala.concurrent.Future

/**
 * Interface to manage Projects
 */
trait IProjectRepository {

  protected val fhirConfigurator: IFhirVersionConfigurator = new FhirR4Configurator()

  /**
   * Retrieve all Projects
   *
   * @return all projects in the repository
   */
  def getAllProjects: Future[Seq[Project]]

  /**
   * Save project to the repository.
   *
   * @param project project to be saved
   * @return the created project
   */
  def createProject(project: Project): Future[Project]

  /**
   * Retrieve the project identified by its id.
   *
   * @param id id of the project
   * @return the project
   */
  def getProject(id: String): Future[Option[Project]]

  /**
   * Update the some fields of project in the repository.
   *
   * @param id      id of the project
   * @param patch patch to be applied to the project
   * @return
   */
  def updateProject(id: String, patch: JObject): Future[Project]

  /**
   * Delete the project from the repository.
   *
   * @param id id of the project
   * @return
   */
  def removeProject(id: String): Future[Unit]
}

/**
 * Keeps file/folder names related to the project repository
 * */
object IProjectRepository {
  val PROJECTS_FOLDER = "projects" // folder keeping the projects
  val PROJECTS_JSON = "projects.json" // file keeping the metadata of all projects
}