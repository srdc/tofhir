package io.tofhir.engine.repository

/**
 * A cached repository.
 */
trait ICachedRepository {
  /**
   * Invalidate the internal cache and refresh the cache content directly from their source
   */
  def invalidate(): Unit
}
