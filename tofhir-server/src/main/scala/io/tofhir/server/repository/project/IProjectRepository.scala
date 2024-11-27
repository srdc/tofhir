package io.tofhir.server.repository.project

import io.onfhir.definitions.common.model.SchemaDefinition
import io.tofhir.engine.model.{FhirMapping, FhirMappingJob}
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
   * Update some fields of the project in the repository.
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
  def deleteProject(id: String): Future[Unit]

  /**
   * Methods to manage the reference from a Project to SchemaDefinitions
   */

  /**
   * Add the schema definition to the project.
   *
   * @param projectId
   * @param schema
   */
  def addSchema(projectId: String, schema: SchemaDefinition): Future[Project]

  /**
   * Replace the schema definition of the project.
   *
   * @param projectId
   * @param schemaMetadata
   */
  def updateSchema(projectId: String, schemaMetadata: SchemaDefinition): Future[Project]

  /**
   * Remove the schema definition from the project
   *
   * @param projectId
   * @param schemaId The unique identifier of the schema to be deleted. If not provided, all schemas will be removed from the project definition.
   */
  def deleteSchema(projectId: String, schemaId: Option[String] = None): Future[Unit]

  /**
   * Methods to manage the reference from a Project to SchemaDefinitions
   */

  /**
   * Add the mapping definition to the project
   *
   * @param projectId Project id to which the mapping will be added to
   * @param mapping Mapping to be added
   */
  def addMapping(projectId: String, mapping: FhirMapping): Future[Project]

  /**
   * Replaces the mapping definition within the project
   *
   * @param projectId Project id of which the mapping will be updated
   * @param mapping Mapping to be updated
   */
  def updateMapping(projectId: String, mapping: FhirMapping): Future[Project]

  /**
   * Deletes the mapping definition from the
   *
   * @param projectId Project id from which the mapping will be deleted
   * @param mappingId The unique identifier of the mapping to be deleted. If not provided, all mappings for the project will be deleted.
   */
  def deleteMapping(projectId: String, mappingId: Option[String] = None): Future[Unit]

  /**
   * Adds the job definition to the project
   *
   * @param projectId
   * @param job
   */
  def addJob(projectId: String, job: FhirMappingJob): Future[Project]

  /**
   * Replaces the job definition within the project
   *
   * @param projectId
   * @param job
   */
  def updateJob(projectId: String, job: FhirMappingJob): Future[Project]

  /**
   * Deletes the job definition of the project
   *
   * @param projectId
   * @param jobId The unique identifier of the job to be deleted. If not provided, all jobs for the project will be deleted.
   */
  def deleteJob(projectId: String, jobId: Option[String] = None): Future[Unit]

  /**
   * Adds the mapping context id to the project
   *
   * @param projectId      Project id the mapping context will be added to
   * @param mappingContext Mapping context id to be added
   */
  def addMappingContext(projectId: String, mappingContext: String): Future[Project]

  /**
   * Deletes the mapping context from the project
   *
   * @param projectId        Project id the mapping context will be deleted
   * @param mappingContextId The unique identifier of the mapping context to be deleted. If not provided, all mapping contexts for the project will be deleted.
   */
  def deleteMappingContext(projectId: String, mappingContextId: Option[String] = None): Future[Unit]

}
