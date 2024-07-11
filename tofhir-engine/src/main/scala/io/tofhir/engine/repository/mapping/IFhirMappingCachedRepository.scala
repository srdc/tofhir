package io.tofhir.engine.repository.mapping

/**
 * A cached repository for the Mappings
 */
trait IFhirMappingCachedRepository extends IFhirMappingRepository {
  /**
   * Invalidate the internal cache and refresh the cache with the FhirMappings directly from their source
   */
  def invalidate(): Unit
}
