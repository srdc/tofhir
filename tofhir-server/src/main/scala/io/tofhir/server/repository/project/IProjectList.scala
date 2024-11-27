package io.tofhir.server.repository.project

/**
 * Interface to handle the projects referred by an entity.
 * This interface is intended to provide the necessary methods to repository implementations (e.g. JobFolderRepository)
 * which relates back to Projects.
 */
trait IProjectList[T] {

  /**
   * Retrieve the projects and entities within.
   * @return Map of projectId -> Seq[T] where T is the entity (e.g. FhirMapping)
   */
  def getProjectPairs: Map[String, Seq[T]]

}
