package io.tofhir.server.repository.project

import io.tofhir.server.model.Project
import org.json4s.JObject

import scala.concurrent.Future

/**
 * Interface to manage Projects
 */
trait IProjectRepository {

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
   * Update some fields of project in the repository.
   *
   * @param id    id of the project
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
