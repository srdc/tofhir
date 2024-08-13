package io.tofhir.server.service

import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.server.model.csv.CsvHeader
import io.tofhir.server.repository.mappingContext.IMappingContextRepository

import scala.concurrent.Future

/**
 * Service for mapping context
 * @param mappingContextRepository
 */
class MappingContextService(mappingContextRepository: IMappingContextRepository) extends LazyLogging {

  /**
   * Get all mapping context ids from the mapping context repository
   * @param projectId return the mapping contexts for the project
   * @return Seq[String]
   */
  def getAllMappingContext(projectId: String): Future[Seq[String]] = {
    mappingContextRepository.getAllMappingContext(projectId)
  }

  /**
   * Save the mapping context to the repository
   * @param projectId project id the mapping context belongs to
   * @param mappingContext mapping context id to save
   * @return String mapping context id
   */
  def createMappingContext(projectId: String, mappingContext: String): Future[String] = {
    mappingContextRepository.createMappingContext(projectId, mappingContext)
  }

  /**
   * Delete the mapping context from the repository
   * @param projectId project id the mapping context belongs to
   * @param id mapping context id
   * @return
   */
  def deleteMappingContext(projectId: String, id: String): Future[Unit] = {
    mappingContextRepository.deleteMappingContext(projectId, id)
  }

  /**
  * Update the mapping context header by its id
  * @param projectId project id the mapping context belongs to
  * @param id mapping context id
  * @param headers mapping context headers
  * @return
  */
  def updateMappingContextHeader(projectId: String, id: String, headers: Seq[CsvHeader]): Future[Unit] = {
    mappingContextRepository.updateMappingContextHeader(projectId, id, headers)
  }

  /**
   * Save the mapping context content to the repository
   * @param projectId project id the mapping context belongs to
   * @param id mapping context id
   * @param byteSource mapping context content to save
   * @return
   */
  def saveMappingContextContent(projectId: String, id: String, byteSource: Source[ByteString, Any], pageNumber: Int, pageSize: Int): Future[Long] = {
    mappingContextRepository.saveMappingContextContent(projectId, id, byteSource, pageNumber, pageSize)
  }

  /**
   * Get the mapping context content by its id
   * @param projectId project id the mapping context belongs to
   * @param id mapping context id
   * @return
   */
  def getMappingContextContent(projectId: String, id: String, pageNumber: Int, pageSize: Int): Future[(Source[ByteString, Any], Long)] = {
    mappingContextRepository.getMappingContextContent(projectId, id, pageNumber, pageSize)
  }

  /**
   * Upload the mapping context content to the repository
   *
   * @param projectId project id the mapping context belongs to
   * @param id        mapping context id
   * @param content   mapping context content to save
   * @return
   */
  def uploadMappingContextFile(projectId: String, id: String, byteSource: Source[ByteString, Any]): Future[Unit] = {
    mappingContextRepository.uploadMappingContext(projectId, id, byteSource)
  }

  /**
   * Download the mapping context content by its id
   * @param projectId project id the mapping context belongs to
   * @param id mapping context id
   * @return
   */
  def downloadMappingContextFile(projectId: String, id: String): Future[Source[ByteString, Any]] = {
    mappingContextRepository.downloadMappingContext(projectId, id)
  }

}
