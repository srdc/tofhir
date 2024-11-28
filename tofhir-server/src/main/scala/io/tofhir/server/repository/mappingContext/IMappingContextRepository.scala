package io.tofhir.server.repository.mappingContext

import akka.stream.scaladsl.Source
import akka.util.ByteString
import io.tofhir.engine.repository.ICachedRepository
import io.tofhir.server.model.csv.CsvHeader
import io.tofhir.server.repository.project.IProjectList

import scala.concurrent.Future

/**
 * Interface for the mapping context repository
 */
trait IMappingContextRepository extends ICachedRepository with IProjectList[String] {

  /**
   * Retrieve the metadata of all mapping context ids
   * @return
   */
  def getAllMappingContext(projectId: String): Future[Seq[String]]

  /**
   * Save the mapping context to the repository.
   * We only store id of a mapping context in the project metadata json file.
   *
   * @param projectId project id the mapping context belongs to
   * @param id mapping context id to save
   * @return
   */
  def createMappingContext(projectId: String, id: String): Future[String]

  /**
   * Delete the mapping context from the repository
   * @param projectId project id the mapping context belongs to
   * @param id mapping context id
   * @return
   */
  def deleteMappingContext(projectId: String, id: String): Future[Unit]

  /**
   * Deletes all mapping contexts associated with a specific project.
   *
   * @param projectId The unique identifier of the project for which mapping contexts should be deleted.
   */
  def deleteAllMappingContexts(projectId: String): Unit

  /**
   * Update the mapping context header by its id
   * @param projectId project id the mapping context belongs to
   * @param id mapping context id
   * @param headers mapping context headers
   * @return
   */
  def updateMappingContextHeader(projectId: String, id: String, headers: Seq[CsvHeader]): Future[Unit]

  /**
   * Save the mapping context content to the repository
   * @param projectId project id the mapping context belongs to
   * @param id mapping context id
   * @param content mapping context content to save
   * @return
   */
  def saveMappingContextContent(projectId: String, id: String, content: Source[ByteString, Any], pageNumber: Int, pageSize: Int): Future[Long]

  /**
   * Get the mapping context content by its id
   * @param projectId project id the mapping context belongs to
   * @param id mapping context id
   * @return
   */
  def getMappingContextContent(projectId: String, id: String, pageNumber: Int, pageSize: Int): Future[(Source[ByteString, Any], Long)]

  /**
   * Upload the mapping context content to the repository
   *
   * @param projectId project id the mapping context belongs to
   * @param id        mapping context id
   * @param content   mapping context content to save
   * @return
   */
  def uploadMappingContext(projectId: String, id: String, content: Source[ByteString, Any]): Future[Unit]

  /**
   * Download the mapping context content by its id
   *
   * @param projectId project id the mapping context belongs to
   * @param id        mapping context id
   * @return
   */
  def downloadMappingContext(projectId: String, id: String): Future[Source[ByteString, Any]]
}
