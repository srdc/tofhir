package io.tofhir.server.service.mappingcontext

import scala.concurrent.Future
import akka.stream.scaladsl.Source
import akka.util.ByteString

/**
 * Interface for the mapping context repository
 */
trait IMappingContextRepository {

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
   * Save the mapping context content to the repository
   * @param projectId project id the mapping context belongs to
   * @param id mapping context id
   * @param content mapping context content to save
   * @return
   */
  def saveMappingContextContent(projectId: String, id: String, content: Source[ByteString, Any]): Future[Unit]

  /**
   * Get the mapping context content by its id
   * @param projectId project id the mapping context belongs to
   * @param id mapping context id
   * @return
   */
  def getMappingContextContent(projectId: String, id: String): Future[Source[ByteString, Any]]
}
