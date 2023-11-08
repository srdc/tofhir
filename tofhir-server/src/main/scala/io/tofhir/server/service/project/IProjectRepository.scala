package io.tofhir.server.service.project

import io.tofhir.server.model.Project
import org.json4s.JObject

import scala.concurrent.Future

/**
 * Interface to manage Projects
 */
trait IProjectRepository {

//  protected val fhirConfigurator: IFhirVersionConfigurator = new FhirR4Configurator()
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
   * Delete the project folders
   *
   * @param id id of the project
   * @return
   */
  def removeProjectFolders(id: String): Future[Unit]

  /**
   * Delete the project from cache and update meta data
   * @param id id of the project
   */
  def removeProjectFromCache(id:String): Unit

  /**
   * Returns IDs of jobs under a project
   *
   * @param id id of the project
   * @return
   */
  def getJobIds(id: String): Seq[String]

  /**
   * Returns IDs of mappings under a project
   *
   * @param id id of the project
   * @return
   */
  def getMappingIds(id: String): Seq[String]

  /**
   * Returns IDs of mapping contexts under a project
   *
   * @param id id of the project
   * @return
   */
  def getMappingContextIds(id: String): Seq[String]

  /**
   * Returns IDs of schemas under a project
   *
   * @param id id of the project
   * @return
   */
  def getSchemaIds(id: String): Seq[String]
}
