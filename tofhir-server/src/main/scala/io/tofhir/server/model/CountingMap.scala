package io.tofhir.server.model

/**
 * An immutable map that keeps track of the count of each key.
 *
 * @param counts The underlying map containing key-value pairs where the key is the element, and the value is the count.
 * @tparam K The type of keys in the map.
 */
case class CountingMap[K](counts: Map[K, Int] = Map.empty[K, Int]) {
  /**
   * Increments the count for the specified key and returns a new CountingMap with the updated counts.
   *
   * @param key The optional key for which the count should be incremented.
   * @return A new CountingMap with the updated counts.
   */
  def apply(key: Option[K]): CountingMap[K] = {
    key match {
      case Some(innerKey) =>
        CountingMap(counts.updated(innerKey, counts.getOrElse(innerKey, 0) + 1))
      case None =>
        // Return the current state for None
        this
    }
  }

  /**
   * Retrieves the count associated with the specified key.
   *
   * @param key The key for which to retrieve the count.
   * @return The count associated with the key, or 0 if the key is not present.
   */
  def get(key: K): Int = {
    counts.getOrElse(key, 0)
  }
}

object CountingMap {
  /**
   * Creates an empty CountingMap.
   *
   * @tparam A The type of keys.
   * @return An empty CountingMap.
   */
  def empty[A]: CountingMap[A] = CountingMap(Map.empty[A, Int])
}