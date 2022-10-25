package io.tofhir.engine.util

import com.google.common.hash.Hashing
import org.apache.commons.codec.binary.StringUtils

object FhirMappingUtility {
  private final val hashSeed = 0x37dd86e4

  /**
   * Create a hashed identifier for given FHIR resource type and external id
   *
   * @param resourceType
   * @param id
   * @return
   */
  def getHashedId(resourceType: String, id: String): String = {
    val inputStr = resourceType + "/" + id
    val hashCode = Hashing.murmur3_128(hashSeed).hashBytes(StringUtils.getBytesUtf8(inputStr))
    hashCode.toString
  }

  def getHashedReference(resourceType: String, id: String): String =
    s"$resourceType/${getHashedId(resourceType, id)}"
}
