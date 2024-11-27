package io.tofhir.common.model

/**
 * A cached repository.
 */
trait ICachedRepository {
  /**
   * Invalidate the internal cache and refresh the cache with the FhirMappings directly from their source
   */
  def invalidate(): Unit
}
